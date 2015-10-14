-module(rmq_consumer_sup).

-behaviour(supervisor).


%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Name, Config) ->
   Name1 = list_to_atom(atom_to_list(Name) ++ "_sup"),
   supervisor:start_link({local, Name1}, ?MODULE, [Config]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([Config]) ->
   RmqWorkers = child_specs(Config),
   {ok, { {one_for_one, 5, 10}, RmqWorkers} }.


%%%%%%%%%% CHILD SPECs %%%%%%%%%%%%%%%%%%%%%
child_specs(Config) ->
   Workers = proplists:get_value(workers, Config),
   Callback = proplists:get_value(callback, Config),

   [{atom_to_list(Callback)++integer_to_list(Number),
      {rmq_consumer, start_link, [Callback, Config]},
      permanent, 5000, worker, dynamic
   } || Number <- lists:seq(1, Workers)].


