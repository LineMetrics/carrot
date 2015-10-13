%% Copyright LineMetrics 2015
-module(carrot).
-author("Alexander Minichmair").

%% API
-export([start/0]).


start() ->
   ok = application:start(crypto),
   ok = application:start(rabbit_common),
   ok = application:start(amqp_client),
   ok = application:start(carrot).
