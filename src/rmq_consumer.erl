%%% @author Alexander Minichmair
%%%
%%% @copyright 2015 LineMetrics GmbH
%%%
%%% @doc MQ consuming - worker.
%%%
%%% every worker holds his own connection and amqp-channel
%%%
%%% rmq_consumer is a behaviour for servers to consume from rabbitMQ
%%% combined with a config for setting up a queue, to consume from (and maybe an exchange, which can be
%%% bound to an existing exchange), a callback module must be implemented with the function(s) defined in the
%%% -callback() clause
%%%
%%%
%%%
%%% @end
%%%
%%%
%%% a gen_server abused to behave like a state-machine in some areas ;)
%%%

-module(rmq_consumer).

-behaviour(gen_server).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Required Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-include_lib("../include/amqp_client.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state, {
   connection = undefined :: undefined|pid(),
   channel = undefined:: undefined|pid(),
   channel_ref = undefined:: undefined|reference(),
   spawned_tasks = []:: [{pid(), reference()}],
   config = []:: proplists:proplist(),
   callback :: atom(),
   callback_state :: term(),
   available = false:: boolean()
}).

-type state():: #state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Exports.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




%%% Public API.

%%% gen_server/worker_pool callbacks.
-export([
   init/1, terminate/2, code_change/3,
   handle_call/3, handle_cast/2, handle_info/2
   , start_link/2, start_monitor/2, stop/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%% BEHAVIOUR DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%%

%%% init the callback
-callback init() -> {ok, ProcessorState :: term()} | {error, Reason :: term()}.

%%% handle a newly arrived amqp message
-callback process(Event :: { #'basic.deliver'{}, #'amqp_msg'{} }, ProcessorState :: term()) ->
   {ok, NewProcessorState} | {ok, noack, NewProcessorState} | {error, Reason :: term(), NewProcessorState}.

%%% handle termination of the process
-callback terminate(TReason :: term(), ProcessorState :: term()) ->
   ok | {error, Reason :: term()}.

%% this callback is optional for handling other info messages for the callback
-callback handle_info(TEvent :: term(), ProcessorState :: term()) ->
   {ok, NewProcessorState :: term()} | {error, Reason :: term()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
stop(Server) ->
   gen_server:call(Server, stop).

start_link(Callback, Config) ->
   lager:notice("Callback: ~p CONFIG : ~p",[Callback, Config]),
   gen_server:start_link(?MODULE, [Callback, Config], []).


start_monitor(Callback, Config) ->
   {ok, Pid} = gen_server:start(?MODULE, [Callback, Config], []),
   erlang:monitor(process, Pid),
   {ok, Pid}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(proplists:proplist()) -> {ok, state()}.
init([Callback, Config]) ->
   process_flag(trap_exit, true),
   {Callback1, CBState} =
   case is_pid(Callback) of
      true  -> {Callback, undefined};
      false -> {ok, CallbackState} = Callback:init(), {Callback, CallbackState}
   end,
   erlang:send_after(0, self(), connect),
   {ok, #state{config = Config, callback = Callback1, callback_state = CBState}}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
   lager:warning("Invalid cast: ~p", [Msg]),
   {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(connect, State) ->
   {Available, Channel, Conn} = check_for_channel(State),

   case Available of
      true  -> carrot_amqp:setup(Channel, State#state.config);
      false -> nil
   end,
   {noreply, State#state{
      channel = Channel,
      available = Available,
      connection = Conn
   }};

handle_info(stop, State=#state{channel = _Channel}) ->
   lager:notice("stopping rmq_consumer: ~p",[self()]),
   {stop, shutdown, State};

handle_info(
    {'DOWN', _MQRef, process, _MQPid, Reason},
    _State=#state{channel = _MQPid}
) ->
   lager:alert("MQ channel is DOWN: ~p", [Reason]);
%%    ,
%%    erlang:send_after(0, self(), connect),
%%    {noreply, State#state{
%%       channel = undefined,
%%       channel_ref = undefined,
%%       available = false
%%    }};


handle_info(
    {'EXIT', MQPid, Reason}, State=#state{channel = MQPid}
) ->
   lager:alert("MQ channel DIED: ~p", [Reason]),
   erlang:send_after(0, self(), connect),
   {noreply, State#state{
      channel = undefined,
      channel_ref = undefined,
      available = false
   }};

handle_info({'EXIT', _OtherPid, _Reason} = Event,
               State=#state{callback = Callback, callback_state = CallbackState} ) ->
   NewCallbackState =
   case erlang:function_exported(Callback, handle_info, 2) of
      true -> {ok, NewCBState} = Callback:handle_info(Event, CallbackState), NewCBState;
      false -> lager:info("Function 'handle_info' not exported in Callback-Module: ~p",[Callback]), CallbackState
   end,

   {noreply, State#state{callback_state = NewCallbackState}};

handle_info(Event = {#'basic.deliver'{}, #'amqp_msg'{}},
            #state{callback = Callback} = State)
                           when is_pid(Callback) ->

   Callback ! {Event, self()},
   {noreply, State};
%% @doc handle incoming messages from rmq
handle_info(Event = {#'basic.deliver'{delivery_tag = DTag, routing_key = _RKey},
   #'amqp_msg'{payload = _Msg, props = #'P_basic'{headers = _Headers}}},
    #state{callback = Callback, callback_state = CState} = State)    ->

   NewCallbackState =
   case Callback:process(Event, CState) of
      {ok, NewState}                -> %lager:info("OK processing queue-message: ~p",[Event]),
         amqp_channel:call(State#state.channel, #'basic.ack'{delivery_tag = DTag}), NewState;

      {ok, noack, NewState}       ->
         NewState;

      {error, _Error, NewState}     -> lager:error("Error when processing queue-message: ~p",[_Error]),
         amqp_channel:call(State#state.channel,
            #'basic.nack'{delivery_tag = DTag, requeue = true, multiple = false}),
         NewState

   end,
   {noreply, State#state{callback_state = NewCallbackState}}
;
handle_info({'basic.consume_ok', Tag}, State) ->
   lager:debug("got handle_info basic.consume_ok for Tag: ~p",[Tag]),
   {noreply, State}
;
handle_info({'basic.qos_ok', {}}, State) ->
   lager:debug("got handle_info basic.qos_ok for Channel: ~p",[State#state.channel]),
   {noreply, State}
;
handle_info({ack, Tag}, State) ->
   amqp_channel:call(State#state.channel, #'basic.ack'{delivery_tag = Tag, multiple = false}),
   lager:debug("acked single Tag: ~p",[Tag]),
   {noreply, State}
;
handle_info({ack, multiple, Tag}, State) ->
   amqp_channel:call(State#state.channel, #'basic.ack'{delivery_tag = Tag, multiple = true}),
   lager:debug("acked multiple till Tag: ~p",[Tag]),
   {noreply, State}
;
handle_info({nack, Tag}, State) ->
   amqp_channel:call(State#state.channel, #'basic.nack'{delivery_tag = Tag, multiple = false, requeue = true}),
   lager:debug("nacked single Tag: ~p",[Tag]),
   {noreply, State}
;
handle_info({nack, multiple, Tag}, State) ->
   amqp_channel:call(State#state.channel, #'basic.nack'{delivery_tag = Tag, multiple = true, requeue = true}),
   lager:debug("nacked multiple till Tag: ~p",[Tag]),
   {noreply, State}
;
handle_info(Msg, State) ->
   lager:error("Unhandled msg in rabbitmq_consumer : ~p", [Msg]),
   {noreply, State}.


handle_call(Req, _From, State) ->
   lager:error("Invalid request: ~p", [Req]),
   {reply, invalid_request, State}.

-spec terminate(atom(), state()) -> ok.
terminate(Reason, #state{
                        callback = Callback,
                        callback_state = CBState,
                        channel = Channel,
                        connection = Conn}) ->

%%    lager:notice("~p ~p terminating with reason: ~p",[?MODULE, self(), Reason]),
   amqp_channel:close(Channel),
   amqp_connection:close(Conn),
   case is_pid(Callback) of
      true -> ok;
      false -> Callback:terminate(Reason, CBState)
   end
   .

-spec code_change(string(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% MQ connection functions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check_for_channel(#state{} = State) ->
   Connect = fun() ->
      case connect(State#state.config) of
         {{ok, Pid},{ok,Conn}} -> {Pid,Conn};
         Error -> lager:alert("MQ NOT available: ~p", [Error]), not_available
      end
   end,
   {Channel, Conn} =
      case State#state.channel of
                {Pid,Conn0} when is_pid(Pid) -> case is_process_alive(Pid) of
                                           true -> {Pid, Conn0};
                                           false -> Connect()
                                        end;
                _ -> Connect()
             end,
%%    lager:notice("new channelpid is ~p",[Channel]),
   Available = is_pid(Channel),
   {Available, Channel, Conn}.

%%%
%%% connect
%%%
connect(Config) ->
   Get = fun
      ({s, X}) ->
         list_to_binary(proplists:get_value(X, Config));
      (X) ->
         proplists:get_value(X, Config) end,
   GetWithDefault = fun(X, Default) ->
      case Get(X) of
         undefined -> Default;
         Value -> Value
      end
   end,
   RabbbitHosts = Get(hosts),
   {A,B,C} = erlang:now(),
   random:seed(A,B,C),
   Index = random:uniform(length(RabbbitHosts)),
   {Host, Port} = lists:nth(Index,RabbbitHosts),
   Connection = amqp_connection:start(#amqp_params_network{
      username = Get({s, user}),
      password = Get({s, pass}),
      virtual_host = Get({s, vhost}),
      port = Port,
      host = Host,
      heartbeat = GetWithDefault(heartbeat, 80),
      ssl_options = GetWithDefault(ssl_options, none)
   }),
   {new_channel(Connection), Connection}.

new_channel({ok, Connection}) ->
%%    link(Connection),
   configure_channel(amqp_connection:open_channel(Connection));

new_channel(Error) ->
   Error.

configure_channel({ok, Channel}) ->
   link(Channel), %erlang:monitor(process, Channel),
   case amqp_channel:call(Channel, #'confirm.select'{}) of
      {'confirm.select_ok'} -> {ok, Channel};
      Error -> lager:warning("Could not configure channel: ~p", [Error]), Error
   end;

configure_channel(Error) ->
   Error.

