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
   , start_link/2]).


%%%%%%%%%%%%%%%%%%%%%%%%%%% BEHAVIOUR DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%% handle a newly arrived amqp message
%%%

-callback init() -> {ok, ProcessorState :: term()} | {error, Reason :: term()}.

-callback process(Event :: { #'basic.deliver'{}, #'amqp_msg'{} }, ProcessorState :: term()) ->
   {ok, NewProcessorState} | {ok, noreply, NewProcessorState} | {error, Reason :: term(), NewProcessorState}.

-callback terminate(TReason :: term(), ProcessorState :: term()) ->
   ok | {error, Reason :: term()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link(Callback, Config) ->
   gen_server:start_link(?MODULE, [Callback, Config], []).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(proplists:proplist()) -> {ok, state()}.
init([Callback, Config]) ->
   {ok, CallbackState} = Callback:init(),
   erlang:send_after(0, self(), connect),
   {ok, #state{config = Config, callback = Callback, callback_state = CallbackState}}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
   lager:error("Invalid cast: ~p", [Msg]),
   {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(connect, State) ->
   {Available, Channel, ChannelRef} = check_for_channel(State),

   case Available of
      true  -> setup(Channel, State#state.config);
      false -> nil
   end,
   {noreply, State#state{
      channel = Channel,
      channel_ref = ChannelRef,
      available = Available
   }};

handle_info(
    {'DOWN', MQRef, process, MQPid, Reason},
    State=#state{channel = MQPid, channel_ref = MQRef}
) ->
   lager:warning("MQ channel is down: ~p", [Reason]),
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
            #'basic.nack'{delivery_tag = DTag, requeue = true}),
         NewState

   end,
   {noreply, State#state{callback_state = NewCallbackState}}
;
handle_info({'basic.consume_ok', Tag}, State) ->
   lager:debug("got handle_info basic.consume_ok for Tag: ~p",[Tag]),
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
terminate(_Reason, _State) ->
   ok.

-spec code_change(string(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% MQ setup and connection functions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
   consume_queue(Channel, QName).

consume_queue(Channel, Q) ->
   #'basic.consume_ok'{consumer_tag = Tag} =
      amqp_channel:subscribe(Channel, #'basic.consume'{queue = Q}, self()),
   lager:info("#setup subscribed to queue : ~p got back tag: ~p~n",[Q, Tag]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


check_for_channel(#state{} = State) ->
   Connect = fun() ->
      case connect(State#state.config) of
         {ok, Pid} -> Pid;
         Error -> lager:alert("MQ NOT available: ~p", [Error]), not_available
      end
   end,
   Channel = case State#state.channel of
                Pid when is_pid(Pid) -> case is_process_alive(Pid) of
                                           true -> Pid;
                                           false -> Connect()
                                        end;
                _ -> Connect()
             end,
   ChannelRef = case Channel of
                   _ when is_pid(Channel) -> erlang:monitor(process, Channel);
                   _ -> erlang:send_after(
                      proplists:get_value(reconnect_timeout, State#state.config), self(), connect
                   ),
                      undefined
                end,
   Available = is_pid(Channel) andalso ChannelRef =/= undefined,
   {Available, Channel, ChannelRef}.

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
   new_channel(amqp_connection:start(#amqp_params_network{
      username = Get({s, user}),
      password = Get({s, pass}),
      virtual_host = Get({s, vhost}),
      port = Port,
      host = Host,
      ssl_options = GetWithDefault(ssl_options, none)
   })).

new_channel({ok, Connection}) ->
   configure_channel(amqp_connection:open_channel(Connection));

new_channel(Error) ->
   Error.

configure_channel({ok, Channel}) ->
   case amqp_channel:call(Channel, #'confirm.select'{}) of
      {'confirm.select_ok'} -> {ok, Channel};
      Error -> lager:warning("Could not configure channel: ~p", [Error]), Error
   end;

configure_channel(Error) ->
   Error.

