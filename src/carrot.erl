%% Copyright LineMetrics 2015
-module(carrot).
-author("Alexander Minichmair").


%% API
-export([start/0]).


start() ->
   application:ensure_all_started(?MODULE, permanent).
