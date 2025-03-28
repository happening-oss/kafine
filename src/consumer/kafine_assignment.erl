-module(kafine_assignment).
-moduledoc false.
-export([]).
-export([
    handle_assignment/5,
    revoke_assignments/3
]).

%% Called once we know what topics and partitions we've been assigned.
%%
%% The assignment callback is called before and after the partitions are assigned.
%% It's used, for example, to create or delete ETS tables.
%%
%% The subscription callback is called to start fetching from the assigned partitions.
handle_assignment(
    NewAssignments,
    PreviousAssignments,
    Connection,
    {AssignmentCallback, AssignmentState0},
    {SubscriptionCallback, SubscriptionState0}
) ->
    {ok, AssignmentState1} = AssignmentCallback:before_assignment(
        NewAssignments, PreviousAssignments, AssignmentState0
    ),

    {ok, SubscriptionState} = SubscriptionCallback:subscribe_partitions(
        Connection, NewAssignments, SubscriptionState0
    ),

    {ok, AssignmentState} = AssignmentCallback:after_assignment(
        NewAssignments, PreviousAssignments, AssignmentState1
    ),

    {ok, SubscriptionState, AssignmentState}.

%% Called when we need to stop fetching after reassignment.
revoke_assignments(
    #{assigned_partitions := RedundantAssignments},
    {SubscriptionCallback, SubscriptionState0},
    {AssignmentCallback, AssignmentState0}
) ->
    {ok, AssignmentState1} = AssignmentCallback:before_assignment(
        #{}, RedundantAssignments, AssignmentState0
    ),

    {ok, SubscriptionState} = SubscriptionCallback:unsubscribe_partitions(SubscriptionState0),

    {ok, AssignmentState} = AssignmentCallback:after_assignment(
        #{}, RedundantAssignments, AssignmentState1
    ),
    {ok, SubscriptionState, AssignmentState}.
