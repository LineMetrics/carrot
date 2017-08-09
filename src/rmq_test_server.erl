%% Copyright LineMetrics 2015
-module(rmq_test_server).
-author("Alexander Minichmair").

-behaviour(gen_server).

%% API
-export([start_link/0]).

-include("../include/amqp_client.hrl").

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {consumer}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
   gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
   {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term()} | ignore).
init([]) ->
   lager:info("~p < ~p > started",[?MODULE, self()]),
   {ok, Pid, _Ref} = rmq_consumer:start_monitor(self(), consumer_config(<<"1.002.eb195daeff594de58a0eaee88cf1190b">>)),

   {ok, #state{consumer = Pid}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
   {reply, Reply :: term(), NewState :: #state{}} |
   {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_call(stop, _From, State) ->
   {stop, shutdown, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
   {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_info({'DOWN', _MonitorRef, process, Consumer, _Info}, #state{consumer = Consumer} = State) ->
   lager:warning("MQ-Consumer:~p is 'DOWN'",[Consumer]),
   {noreply, State#state{consumer = undefined}};
handle_info({_Event = {#'basic.deliver'{delivery_tag = DTag, routing_key = _RKey},
   #'amqp_msg'{payload = Payload, props = #'P_basic'{headers = _Headers}}}, From}, #state{consumer = From}=State) ->
   lager:debug("** got q-message: ~p from: ~p",[Payload, From]),
   carrot:ack(From, DTag),
   {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
   ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
   {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
consumer_config(Sid) when is_binary(Sid) ->
   {ok, HostParams} = application:get_env(carrot, broker),
   Config =
      [
         {workers, 1}, % Number of connections,
         {callback, self()},
         {setup_type, permanent},
         {setup,
            [
               {queue, [
                  {queue, <<"qm_ruledata_", Sid/binary>>},
                  {exchange, <<"x_ds_topic">>},
                  {routing_key, Sid}
               ]}
            ]
         }


      ],
   carrot_util:proplists_merge(HostParams, Config).