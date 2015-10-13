%%% @author Alexander Minichmair
%%%
%%% @copyright 2015 LineMetrics GmbH
%%%
%%% @doc MQ consuming - worker.
%%%
%%% rmq_consumer is a behaviour for servers to consume from rabbitMQ
%%% combined with a config for setting up a queue, to consume from and maybe an exchange which can be
%%% bound to an existing exchange, a callback module must be implemented with the function(s) defined in the
%%% -callback() clause
%%%
%%% example config :
%%%
%% {mq_worker, [
%% {callback, rmq_test},
%% {setup,
%% [
%% {exchange, [
%% {exchange, <<"x_gen_consumer_test1">>}, {destination, <<"x_gen_consumer_test1">>},
%% {type, <<"topic">>},
%% {source, <<"x_ds_fanout">>}, {routing_key, <<"some.key">>},
%% {durable, false}, {auto_delete, true},
%% {arguments, []}
%% ]
%% },
%% {queue, [
%% {queue, <<"q_gen_consumer_test1">>},
%% {exchange, <<"x_gen_consumer_test1">>},
%% {durable, false}, {auto_delete, true},
%% {routing_key, <<"some.routing.key">>}, {arguments,[]}
%% ]}
%% ]
%% },
%% {workers, 2}, % Number of connections/consumers/channels,
%% {hosts, [ {"127.0.0.1",5672} ]},
%% {user, "youruser"},
%% {pass, "yourpass"},
%% {vhost, "/"},
%% {confirm_timeout, 1000},
%% {reconnect_timeout, 5000},
%% {ssl_options, none} % Optional. Can be 'none' or [ssl_option()]
%% ]}
%%%
%%% in your supervisor, you would start this consumer setup with : rmq_consumer:child_spec(YourAppName, mq_worker) ->
%%% this gives you a list of child-specs for a supervisor with 2 workers
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
   , start_link/2, child_spec/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%% BEHAVIOUR DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%% handle a newly arrived amqp message
%%%
-callback process(Event :: { #'basic.deliver'{}, #'amqp_msg'{} }) ->
   ok | {error, Reason :: term()}.


%%%%%%%%%% CHILD SPEC %%%%%%%%%%%%%%%%%%%%%
child_spec(Config) ->
   lager:alert("~nConfig for carrot: ~p~n",[Config]),
   Workers = proplists:get_value(workers, Config),
   Callback = proplists:get_value(callback, Config),

   [{atom_to_list(Callback)++integer_to_list(Number),
      {?MODULE, start_link, [Callback, Config]},
      permanent, 5000, worker, dynamic
   } || Number <- lists:seq(1, Workers)].

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

   erlang:send_after(0, self(), connect),
   {ok, #state{config = Config, callback = Callback}}.

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
    #state{callback = Callback}=State)    ->

%%    lager:alert("Rabbit-Consumer-Worker [~p] got message delivered with"
%%    ++" Routing-Key:~p and Message: ~p~n Headers are: ~p",[self(), RKey, Msg, Headers]),
   %% ack msg
   M = #'basic.ack'{delivery_tag = DTag},
   amqp_channel:call(State#state.channel, M),

   case Callback:process(Event) of
      ok                   -> {noreply, State};
      {error, _Error}      -> lager:error("Error when processing queue-message: ~p",[_Error]),
         {noreply, State}
   end
;
handle_info({'basic.consume_ok', Tag}, State) ->
   lager:debug("got handle_info basic.consume_ok for Tag: ~p",[Tag]),
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
%%    lager:alert("Config for ~p : ~p",[?MODULE, Config]),
   Setup = proplists:get_value(setup, Config),
   case proplists:get_value(exchange, Setup) of
      undefined -> ok;
      XCreateConfig ->  %% declare and bind exchange to exchange
         XDeclare = to_exchange_declare(XCreateConfig),
         #'exchange.declare_ok'{} = amqp_channel:call(Channel, XDeclare),
         XBind = to_exchange_bind(XCreateConfig),
         #'exchange.bind_ok'{} = amqp_channel:call(Channel, XBind)

   end,

   QConfig = proplists:get_value(queue, Setup),

   %% declare and bind queue to exchange
   QDeclare = to_queue_declare(QConfig),

   #'queue.declare_ok'{queue = QName} = amqp_channel:call(Channel, QDeclare),
   QBind = to_queue_bind(QConfig),
   #'queue.bind_ok'{} = amqp_channel:call(Channel, QBind),

   consume_queue(Channel, QName).

consume_queue(Channel, Q) ->
   #'basic.consume_ok'{consumer_tag = Tag} =
      amqp_channel:subscribe(Channel, #'basic.consume'{queue = Q}, self()),
   lager:debug("subsribed to queue : ~p got back tag: ~p~n",[Q, Tag]).

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


-spec qx_name(binary()) -> binary().
qx_name(Prefix) ->
   NodeBinary = list_to_binary(atom_to_list(node())),
   Node = binary:replace(NodeBinary, <<"@">>, <<"_">>),
   <<Prefix/binary, <<"_">>/binary, Node/binary>>.

%% Converts a tuple list of values to a queue.declare record
-spec to_exchange_declare([{atom(), term()}]) -> #'exchange.declare'{}.
to_exchange_declare(Props) ->
   %% This is a safety in case certain arguments aren't set elsewhere
   NFields =
      case proplists:get_value(name_postfix, Props) of
         true  -> [exchange, destination];
         _B    -> []
      end,
   Props1 = expand_names(NFields, Props),

   Defaults = [ {ticket,0}, {arguments,[]} ],
   Enriched = lists:merge(Props1, Defaults),
   list_to_tuple(['exchange.declare'|[proplists:get_value(X,Enriched,false) ||
      X <- record_info(fields,'exchange.declare')]]).

%% Converts a tuple list of values to a queue.declare record
-spec to_queue_declare([{atom(), term()}]) -> #'queue.declare'{}.
to_queue_declare(Props) ->
   %% This is a safety in case certain arguments aren't set elsewhere
   NFields =
      case proplists:get_value(name_postfix, Props) of
         true  -> [queue];
         _B    -> []
      end,

   Props1 = expand_names(NFields, Props),

   Defaults = [ {ticket,0}, {arguments,[]} ],
   Enriched = lists:merge(Props1, Defaults),
   list_to_tuple(['queue.declare'|[proplists:get_value(X,Enriched,false) ||
      X <- record_info(fields,'queue.declare')]]).

to_exchange_bind(Props) ->
   Defaults = [ {ticket,0}, {arguments,[]} ],
   NFields0 =
      case proplists:get_value(name_postfix, Props) of
         true  -> [exchange, destination];
         _B    -> []
      end,
   Props1 = expand_names(NFields0, Props),
   Enriched = lists:merge(Props1, Defaults),
   list_to_tuple(['exchange.bind'|[proplists:get_value(X,Enriched,false) ||
      X <- record_info(fields,'exchange.bind')]]).

to_queue_bind(Props) ->
   Defaults = [ {ticket,0}, {arguments,[]} ],
   NFields =
      case proplists:get_value(xname_postfix, Props) of
         true  -> [queue, exchange];
         _F    -> []
      end,
   Props1 = expand_names(NFields, Props),
   Enriched = lists:merge(Props1, Defaults),
   list_to_tuple(['queue.bind'|[proplists:get_value(X,Enriched,false) ||
      X <- record_info(fields,'queue.bind')]]).



expand_names([], Props) ->
   Props;
expand_names([Field | R], Props) ->
%%    lager:debug("Expand name : ~p " ,[Field]),
   Val0 = proplists:get_value(Field, Props),
   Props1 = lists:keystore(Field, 1, Props, {Field, qx_name(Val0)}),
%%    lager:debug("Props before expanding: ~p ~n after expanding: ~p~n",[Props, Props1]),
   expand_names(R, Props1).