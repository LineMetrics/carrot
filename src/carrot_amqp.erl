%% Copyright LineMetrics 2015
-module(carrot_amqp).
-author("Alexander Minichmair").

-include_lib("../include/amqp_client.hrl").

%% API


-export([to_exchange_declare/2, to_queue_declare/2, to_exchange_bind/2, to_queue_bind/2]).

%%% amqp msg default definitions

-define(BASE_ARGS,   [{ticket,0}, {arguments,[]}] ).

-define(Q_TEMP_ARGS, ?BASE_ARGS ++ [{durable, false}, {auto_delete, true}]).
-define(Q_PERM_ARGS, ?BASE_ARGS ++ [{durable, true}, {auto_delete, false}]).

-define(X_TEMP_ARGS, ?Q_TEMP_ARGS).
-define(X_PERM_ARGS, ?Q_PERM_ARGS).

-define(BIND_ARGS,   [{ticket,0}, {routing_key, <<"#">>}, {arguments,[]}] ).


-define(X_DECLARE,   'exchange.declare').
-define(Q_DECLARE,   'queue.declare').
-define(X_BIND,      'exchange.bind').
-define(Q_BIND,      'queue.bind').


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% Converts a tuple list of values to a queue.declare record
-spec to_exchange_declare([{atom(), term()}], atom()) -> #'exchange.declare'{}.
to_exchange_declare(Props, Type) ->
   {NFields, Defaults} = prep_x_declare(Type),
   Props1 = name_postfix(NFields, Props),
   to_record(?X_DECLARE, Props1, Defaults).

prep_x_declare(temporary) ->
   {[exchange], ?X_TEMP_ARGS};
prep_x_declare(permanent) ->
   {[], ?X_PERM_ARGS}.


%% Converts a tuple list of values to a queue.declare record
-spec to_queue_declare([{atom(), term()}], atom()) -> #'queue.declare'{}.
to_queue_declare(Props, Type) ->
   {NFields, Defaults} = prep_q_declare(Type),
   Props1 = name_postfix(NFields, Props),
   to_record(?Q_DECLARE, Props1, Defaults).

prep_q_declare(temporary) ->
   {[queue], ?Q_TEMP_ARGS};
prep_q_declare(permanent) ->
   {[], ?Q_PERM_ARGS}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Converts a tuple list of values to a exchange.bind record
to_exchange_bind(Props, Type) ->
   {NFields, Defaults0} = prep_x_bind(Type),
   Props1 = name_postfix(NFields, Props),
   %% inject exchange-name as destination via defaults
   Destination = proplists:get_value(exchange, Props1),
   Defaults = lists:keystore(destination, 1, Defaults0, {destination, Destination}),
   to_record(?X_BIND, Props1, Defaults).

prep_x_bind(temporary) ->
   {[exchange],  ?BIND_ARGS};
prep_x_bind(permanent) ->
   {[], ?BIND_ARGS}.


%% Converts a tuple list of values to a queue.bind record
to_queue_bind(Props, Type) ->
   XNamePostFix = proplists:get_value(xname_postfix, Props, false),
   NFields = prep_q_bind(Type, XNamePostFix),
   Props1 = name_postfix(NFields, Props),
   to_record(?Q_BIND, Props1, ?BIND_ARGS).

prep_q_bind(temporary, true) ->
   [exchange, queue];
prep_q_bind(permanent, true) ->
   [exchange];
prep_q_bind(permanent, false) ->
   [];
prep_q_bind(temporary, false) ->
   [queue].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Converts a tuple list of values to a record with name RecName
to_record(RecName, Properties, Defaults) ->
   to_record(RecName, carrot_util:proplists_merge(Properties, Defaults)).
to_record(RecName, Properties) ->
   list_to_tuple([RecName|[proplists:get_value(X, Properties, false) ||
      X <- recInfo(RecName)]]).

%% this is ugly, but erlang records are a compiler-hack you know
recInfo('exchange.declare') ->
   record_info(fields, 'exchange.declare');
recInfo('queue.declare') ->
   record_info(fields, 'queue.declare');
recInfo('exchange.bind') ->
   record_info(fields, 'exchange.bind');
recInfo('queue.bind') ->
   record_info(fields, 'queue.bind').


name_postfix([], Props) ->
   Props;
name_postfix([Field | R], Props) ->
   Val0 = proplists:get_value(Field, Props),
   Props1 = lists:keystore(Field, 1, Props, {Field, qx_name(Val0)}),
   name_postfix(R, Props1).


-spec qx_name(binary()) -> binary().
qx_name(Prefix) ->
   NodeBinary = list_to_binary(atom_to_list(node())),
   Node = binary:replace(NodeBinary, <<"@">>, <<"-">>),
   <<Prefix/binary, <<"_">>/binary, Node/binary>>.