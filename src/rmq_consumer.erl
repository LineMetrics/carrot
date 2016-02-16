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
   , start_link/2, kill_channel/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%% BEHAVIOUR DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%% handle a newly arrived amqp message
%%%

-callback init() -> {ok, ProcessorState :: term()} | {error, Reason :: term()}.

-callback process(Event :: { #'basic.deliver'{}, #'amqp_msg'{} }, ProcessorState :: term()) ->
   {ok, NewProcessorState} | {ok, noack, NewProcessorState} | {error, Reason :: term(), NewProcessorState}.

-callback terminate(TReason :: term(), ProcessorState :: term()) ->
   ok | {error, Reason :: term()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
kill_channel(Server) ->
   gen_server:call(Server, kill).

start_link(Callback, Config) ->
   gen_server:start_link(?MODULE, [Callback, Config], []).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(proplists:proplist()) -> {ok, state()}.
init([Callback, Config]) ->
%%    lager:notice("Starting rmq_consumer ..."),
   process_flag(trap_exit, true),
   {ok, CallbackState} = Callback:init(),
   erlang:send_after(0, self(), connect),
%%    erlang:send_after(3000, self(), kill),
   {ok, #state{config = Config, callback = Callback, callback_state = CallbackState}}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
   lager:error("Invalid cast: ~p", [Msg]),
   {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(connect, State) ->
   {Available, Channel, Conn} = check_for_channel(State),

   case Available of
      true  -> setup(Channel, State#state.config);
      false -> nil
   end,
   {noreply, State#state{
      channel = Channel,
      available = Available,
      connection = Conn
   }};

handle_info(kill, State=#state{channel = _Channel}) ->
   lager:notice("kill the channel"),
%%    exit(Channel, aus),
%%    R = amqp_channel:close(Channel),
%%    lager:info("closing channel gives: ~p",[R]),
   {stop, die, State};

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

handle_call(kill, _From, State=#state{channel = Channel}) ->
   lager:error("kill the channel"),
   amqp_channel:close(Channel),
   {reply, ok, State};
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
   Callback:terminate(Reason, CBState).

-spec code_change(string(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% MQ setup and connection functions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% setup exchange, queue, bind exchange queue, setup prefetch and consume from the queue
setup(Channel, Config) ->

   Setup = proplists:get_value(setup, Config),
   Type = proplists:get_value(setup_type, Config),
   case proplists:get_value(exchange, Setup) of
      undefined -> ok; %% if there is no xchange defined, just declare the mandatory queue

      XCreateConfig ->  %% declare and bind exchange to exchange1
         XDeclare = carrot_amqp:to_exchange_declare(XCreateConfig, Type),
         #'exchange.declare_ok'{} = amqp_channel:call(Channel, XDeclare),
         lager:info("#setup Xchange: ~p",["xchange declared ok"]),
         XBind = carrot_amqp:to_exchange_bind(XCreateConfig, Type),
         #'exchange.bind_ok'{} = amqp_channel:call(Channel, XBind),
         lager:info("#setup Xchange: ~p",["xchange bind ok"])
   end,

   QConfig = proplists:get_value(queue, Setup),

   %% declare and bind queue to exchange
   QDeclare = carrot_amqp:to_queue_declare(QConfig, Type),

   #'queue.declare_ok'{queue = QName} = amqp_channel:call(Channel, QDeclare),
   lager:info("#setup Queue: ~p ~n ~p",["queue declared ok", QDeclare]),
   case proplists:get_value(exchange, QConfig) of
      undefined   -> ok;
      _E          ->
         QBind = carrot_amqp:to_queue_bind(QConfig, Type),
         #'queue.bind_ok'{} = amqp_channel:call(Channel, QBind),
         lager:info("#setup Queue: ~p ~n ~n ~p",["queue bind ok", QBind])
   end,
   consume_queue(Channel, QName, proplists:get_value(prefetch_count, Config, 0)).

consume_queue(Channel, Q, Prefetch) ->
   %% set prefetch count if any
   case Prefetch > 0 of
      false   ->
         ok;
      true      ->
         lager:info("Set Prefetch-Count for Channel: ~p",[Prefetch]),
         #'basic.qos_ok'{} = amqp_channel:call(Channel, #'basic.qos'{prefetch_count = Prefetch})
   end,
   %% actually consume from q
   #'basic.consume_ok'{consumer_tag = Tag} =
         amqp_channel:subscribe(Channel, #'basic.consume'{queue = Q}, self()),
   lager:info("#setup subscribed to queue : ~p got back tag: ~p~n",[Q, Tag]).

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

