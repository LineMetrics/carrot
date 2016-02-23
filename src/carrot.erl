%% Copyright LineMetrics 2015
-module(carrot).
-author("Alexander Minichmair").


%% API
-export([start/0, load_bunnies/0, load_bunnies/1]).

%% User API
-export([ack/2, ack_multiple/2, nack/2, nack_multiple/2]).


start() ->
   application:ensure_all_started(?MODULE, permanent).


%% reload bunny definition from a config-file
load_bunnies() ->
   carrot_sup:load_bunnies().
load_bunnies(File) ->
   carrot_sup:load_bunnies(File).

ack(Consumer, Tag) ->
   Consumer ! {ack, Tag}.
ack_multiple(Consumer, Tag) ->
   Consumer ! {ack, multiple, Tag}.
nack(Consumer, Tag) ->
   Consumer ! {nack, Tag}.
nack_multiple(Consumer, Tag) ->
   Consumer ! {nack, multiple, Tag}.