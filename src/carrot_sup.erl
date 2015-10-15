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
   {ok, HostParams} = application:get_env(carrot, broker),

   SetupNames = proplists:get_keys(Config),

   Children = [rmq_sup(Name, Config, HostParams) || Name <- SetupNames],

   {ok, { {one_for_one, 5, 10}, Children} }.


%% Supervisor Definition for rmq_consumer workers
rmq_sup(SetupName, Config, HostParams) ->
   WorkerConf0 = proplists:get_value(SetupName, Config),
%%    lager:alert("Worker-Conf0 :~p",[WorkerConf0]),
   %% inject host-params to worker-config
   WorkerConf = carrot_util:proplists_merge(WorkerConf0, HostParams),
%%    lager:alert("Worker-Conf :~p",[WorkerConf]),
   {SetupName,
      {rmq_consumer_sup, start_link, [SetupName, WorkerConf]},
      permanent, brutal_kill, supervisor, [rmq_consumer_sup]
   }.