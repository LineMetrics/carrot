%% Copyright LineMetrics 2015
-module(rmq_test).
-author("Alexander Minichmair").

-behaviour(rmq_consumer).

-include("../include/amqp_client.hrl").
%% API
-export([process/2, init/0, terminate/2, handle_info/2]).

-record(state, {}).

init() ->
%%   rmq_test_server:start_link(),
   {ok, #state{}}.

process( {Event = #'basic.deliver'{delivery_tag = _DTag, routing_key = _RKey},
         Msg = #'amqp_msg'{payload = _Msg, props = #'P_basic'{headers = _Headers}}} , #state{} = State) ->

   lager:debug("~p got message to PROCESS ::: ~p ~n ~p",[?MODULE, Event, Msg]),
   {ok, State}.

handle_info({'EXIT', MQPid, Reason}, State ) ->
   lager:warning("Pid: ~p exited with Reason: ~p",[MQPid, Reason]),
   {ok, State}.

terminate(_Reason, _State) ->
   lager:debug("~p got terminate message with reason: ~p",[?MODULE, _Reason]),
   ok.