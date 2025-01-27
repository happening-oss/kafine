-module(kafine_assignment_callback).

-callback init(Args :: term()) -> {ok, State :: term()}.
-callback before_assignment(
    NewAssignment :: #{ kafine:topic() => [kafine:partition()]},
    PreviousAssignment :: #{ kafine:topic() => [kafine:partition()]},
    State
) ->
    {ok, State}.
-callback after_assignment(
    NewAssignment :: #{ kafine:topic() => [kafine:partition()]},
    PreviousAssignment :: #{ kafine:topic() => [kafine:partition()]},
    State
) ->
    {ok, State}.
