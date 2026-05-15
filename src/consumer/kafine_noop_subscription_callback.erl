-module(kafine_noop_subscription_callback).
-behaviour(kafine_subscription_callback).

-export([
    init/1,
    subscribe_partitions/3,
    unsubscribe_partitions/1
]).

init(_) ->
    {ok, undefined}.

subscribe_partitions(_Coordinator, _Assignment, State) ->
    {ok, State}.

unsubscribe_partitions(State) ->
    {ok, State}.
