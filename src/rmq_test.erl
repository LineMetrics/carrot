%% Copyright LineMetrics 2015
-module(rmq_test).
-author("Alexander Minichmair").

-behaviour(rmq_consumer).

-include("../include/amqp_client.hrl").
%% API
-export([process/1]).


process( {Event = #'basic.deliver'{delivery_tag = _DTag, routing_key = _RKey},
         Msg = #'amqp_msg'{payload = _Msg, props = #'P_basic'{headers = _Headers}}}) ->

   lager:debug("~p got message to PROCESS ::: ~p ~n ~p",[?MODULE, Event, Msg]),
   ok.