-module(carrot_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
   {ok, Config} = application:get_env(carrot, bunnies),

   SetupNames = proplists:get_keys(Config),

   Children = [rmq_sup(Name, Config) || Name <- SetupNames],

   {ok, { {one_for_one, 5, 10}, Children} }.


%% Supervisor Definition for rmq_consumer workers
rmq_sup(SetupName, Config) ->
   WorkerConf = proplists:get_value(SetupName, Config),
   {SetupName,
      {rmq_consumer_sup, start_link, [SetupName, WorkerConf]},
      permanent, brutal_kill, supervisor, [rmq_consumer_sup]
   }.