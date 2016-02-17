-module(carrot_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, load_bunnies/0, load_bunnies/1]).

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
      permanent, infinity, supervisor, [rmq_consumer_sup]
   }.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% CONFIG-UPDATE
%%%


load_bunnies() ->
   load_bunnies("./sys.config").

load_bunnies(File) ->
   {Config, Names} = parse_file(File),
   {ok, HostParams} = application:get_env(carrot, broker)
   ,
   Children = [rmq_sup(Name, Config, HostParams) || Name <- Names],
   OldChildren = supervisor:which_children(?MODULE),
   OldChildNames = proplists:get_keys(OldChildren),

   Check = fun({SName, {rmq_consumer_sup, start_link, [_SetupName, WorkerConf]}, _,_,_,_} = Child) ->
      case lists:member(SName, OldChildNames) of
         true -> %% if supervisor already exists then add or remove workers
            {value, {SName, SPid, _, _}, _A} = lists:keytake(SName, 1, OldChildren),
            RSupChildren = supervisor:which_children(SPid),
            OldNumWorkers = length(RSupChildren),
            NewNumWorkers = proplists:get_value(workers, WorkerConf),
            case OldNumWorkers > NewNumWorkers of
               true -> %% remove workers
                  rmq_consumer_sup:terminate_workers(SPid, WorkerConf, NewNumWorkers+1, OldNumWorkers),
                  io:format("REMOVING ~p worker(s) from : '~p' <~p>~n",[OldNumWorkers-NewNumWorkers, SName, SPid]),
                  ok;
               false -> %% start all workers
                  case OldNumWorkers == NewNumWorkers of
                     true  -> ok;
                     false -> io:format("ADDING ~p worker(s) to : '~p' <~p>~n",[NewNumWorkers-OldNumWorkers, SName, SPid]),
                        rmq_consumer_sup:add_workers(SPid, WorkerConf)
                  end
            end;
         false -> %% no, start supervisor
            supervisor:start_child(?MODULE, Child),
            io:format("STARTING new '~p'~n",[SName])
      end
   end,
   lists:foreach(Check, Children),
   %% check for rmq_consumer_sups that are not in the config anymore -> stop them supervisors
   ToRemove = lists:subtract(OldChildNames, Names),
   case length(ToRemove) > 0 of
      true -> %% stop all these supervisors
         DelFun = fun(SName) ->
            {value, {SName, Sup, _, _}, _A} = lists:keytake(SName, 1, OldChildren),
            OldNumWorkers = supervisor:count_children(Sup),
            io:format("STOPPING : '~p' <~p> with ~p workers ~n",[SName, Sup, proplists:get_value(workers, OldNumWorkers)]),
            supervisor:terminate_child(?MODULE, SName), supervisor:delete_child(?MODULE, SName)
         end,
         lists:foreach(DelFun, ToRemove),
         ok;
      false -> ok
   end
.


parse_file(File) ->
   io:format("~n... load bunnies from file : ~p~n~n",[File]),
   {ok, [Conf]} = file:consult(File),
%%    lager:notice("read file: ~p",[Conf]),
   Carrot = proplists:get_value(carrot, Conf),
%%    lager:notice("carrot is: ~p",[Carrot]),
   Config = proplists:get_value(bunnies, Carrot),
%%    lager:notice("config is ~p",[Config]),
   Names = proplists:get_keys(Config),
   io:format("new names are: ~p~n",[Names]),
   {Config, Names}.




%% #{id => child_id(),       % mandatory
%% start => mfargs(),      % mandatory
%% restart => restart(),   % optional
%% shutdown => shutdown(), % optional
%% type => worker(),       % optional
%% modules => modules()}   % optional