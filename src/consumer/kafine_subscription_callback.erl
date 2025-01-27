-module(kafine_subscription_callback).

-callback init(Args :: term()) -> {ok, State :: term()}.
-callback subscribe_partitions(Coordinator :: kafine:connection(), Assignment :: #{ kafine:topic() => [kafine:partition()]}, State) ->
    {ok, State}.
-callback unsubscribe_partitions(State) ->
    {ok, State}.
