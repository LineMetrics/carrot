%% Copyright LineMetrics 2015
-module(carrot_amqp).
-author("Alexander Minichmair").

-include_lib("../include/amqp_client.hrl").

%% API
-compile(export_all).


%% Converts a tuple list of values to a queue.declare record
-spec to_exchange_declare([{atom(), term()}], atom()) -> #'exchange.declare'{}.
to_exchange_declare(Props, Type) ->
   {NFields, Defaults} = prep_x_declare(Type),
   Props1 = name_postfix(NFields, Props),
   Enriched = lists:merge(Props1, Defaults),
   list_to_tuple(['exchange.declare'|[proplists:get_value(X,Enriched,false) ||
      X <- record_info(fields,'exchange.declare')]]).

prep_x_declare(temporary) ->
   {[exchange], [ {ticket,0}, {arguments,[]}, {durable, false}, {auto_delete, true}]};
prep_x_declare(permanent) ->
   {[], [ {ticket,0}, {arguments,[]}, {durable, true}, {auto_delete, false}]}.

%% Converts a tuple list of values to a queue.declare record
-spec to_queue_declare([{atom(), term()}], atom()) -> #'queue.declare'{}.
to_queue_declare(Props, Type) ->
   {NFields, Defaults} = prep_q_declare(Type),
   Props1 = name_postfix(NFields, Props),
   Enriched = lists:merge(Props1, Defaults),
   list_to_tuple(['queue.declare'|[proplists:get_value(X,Enriched,false) ||
      X <- record_info(fields,'queue.declare')]]).

prep_q_declare(temporary) ->
   {[queue], [ {ticket,0}, {arguments,[]}, {durable, false}, {auto_delete, true}]};
prep_q_declare(permanent) ->
   {[], [ {ticket,0}, {arguments,[]}, {durable, true}, {auto_delete, false}]}.

to_exchange_bind(Props, Type) ->
   {NFields, Defaults0} = prep_x_bind(Type),
   Props1 = name_postfix(NFields, Props),

   %% inject exchange-name as destination via defaults
   Destination = proplists:get_value(exchange, Props1),
   Defaults = lists:keystore(destination, 1, Defaults0, {destination, Destination}),
   Enriched = lists:merge(Props1, Defaults),
   list_to_tuple(['exchange.bind'|[proplists:get_value(X,Enriched,false) ||
      X <- record_info(fields,'exchange.bind')]]).

prep_x_bind(temporary) ->
   {[exchange], [ {ticket,0}, {arguments,[]}, {routing_key, <<"">>}]};
prep_x_bind(permanent) ->
   {[], [ {ticket,0}, {arguments,[]}, {routing_key, <<"">>}]}.

to_queue_bind(Props, Type, XNamePostFix) ->
   Defaults = [ {ticket,0}, {arguments,[]}],
   NFields = prep_q_bind(Type, XNamePostFix),
   Props1 = name_postfix(NFields, Props),
   Enriched = lists:merge(Props1, Defaults),
   list_to_tuple(['queue.bind'|[proplists:get_value(X,Enriched,false) ||
      X <- record_info(fields,'queue.bind')]]).

prep_q_bind(temporary, true) ->
   [exchange, queue];
prep_q_bind(permanent, true) ->
   [exchange];
prep_q_bind(permanent, false) ->
   [];
prep_q_bind(temporary, false) ->
   [queue].


name_postfix([], Props) ->
   Props;
name_postfix([Field | R], Props) ->
%%    lager:debug("Expand name : ~p " ,[Field]),
   Val0 = proplists:get_value(Field, Props),
   Props1 = lists:keystore(Field, 1, Props, {Field, qx_name(Val0)}),
%%    lager:debug("Props before expanding: ~p ~n after expanding: ~p~n",[Props, Props1]),
   name_postfix(R, Props1).


-spec qx_name(binary()) -> binary().
qx_name(Prefix) ->
   NodeBinary = list_to_binary(atom_to_list(node())),
   Node = binary:replace(NodeBinary, <<"@">>, <<"_">>),
   <<Prefix/binary, <<"_">>/binary, Node/binary>>.