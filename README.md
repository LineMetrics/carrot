### Carrot

Carrot is a small Erlang app that helps Erlang Systems to easly consume and work on messages 
received from a rabbit_mq-broker by providing a callback module that handles the incoming messages.

All this is done via a simple proplist-config which is also updateable on the fly.

Note: carrot is fairly new, so things may change

STATUS
------
we already use carrot in production, stability and throughput is satisfying
@ todo documentation !!!

## The Configuration

#

    [

      {carrot,
        [
           %% connection parameters
           {broker, [
              {hosts, [ {"192.168.0.1",5672} ]},
              {user, "user"},
              {pass, "pass"},
              {vhost, "/"},
              {reconnect_timeout, 4000},
              {ssl_options, none} % Optional. Can be 'none' or [ssl_option()]
             ]},
           %% consumer setups
           {bunnies,
              [
                 %% cassandra topics
                 {cass_worker_1005, [
                    {workers, 3}, % Number of connections,
                    {callback, rmq_test},
                    {setup_type, temporary},
                    {prefetch_count, 40},
                    {setup,
                       [
                          {queue, [
                             {queue, <<"cassandra_1005">>},
                             {exchange, <<"x_cassandra_topic">>}, {xname_postfix, false},
                             {routing_key, <<"5.#">>},
                             %% optional bindings - parameter, if this is given, the q-binding-setup
                             %% will use these routing-keys for binding, instead of the "routing_key" param
                             {bindings, [<<"5.123">>, <<"5.234">>]}
                          ]}
                       ]
                    }


                 ]}


         ]} %% end bunnies

      ]} %% end carrot


    ]

#

## Behaviour Callbacks

Implement the rmq_consumer behaviour (depending on the usage-strategy you use, see rmq_consumer.erl)

#

    %%% init the callback
    -callback init() -> {ok, ProcessorState :: term()} | {error, Reason :: term()}.

    %%% handle a newly arrived amqp message
    -callback process(Event :: { #'basic.deliver'{}, #'amqp_msg'{} }, ProcessorState :: term()) ->
       {ok, NewProcessorState} | {ok, noack, NewProcessorState} | {error, Reason :: term(), NewProcessorState}.

    %%% handle termination of the process
    -callback terminate(TReason :: term(), ProcessorState :: term()) ->
       ok | {error, Reason :: term()}.

    %% this callback is optional for handling other messages
    -callback handle_info(TEvent :: term(), ProcessorState :: term()) ->
       {ok, NewProcessorState} | {error, Reason :: term()}.

#

