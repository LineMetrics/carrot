%% Copyright LineMetrics 2015
-module(carrot_util).
-author("Alexander Minichmair").

%% API
-export([proplists_merge/2]).


proplists_merge(L, T) ->
   lists:ukeymerge(1, lists:keysort(1,L), lists:keysort(1,T)).