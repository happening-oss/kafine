-module(kafine_range_assignor).
-behaviour(kafine_assignor).

%%% Simplified implementation of Java's RangeAssignor.
%%% - NOT rack-aware.
%%% - does NOT use group instance ID.

-export([
    name/0,
    metadata/1,
    assign/3
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

name() -> <<"range">>.

-spec metadata(Topics :: [kafine:topic()]) -> kafine_assignor:metadata().

%% Topics is the list of topics that we want to subscribe to.
%% UserData is currently unused.
%% We assume version 0 of the consumer subscription.
%% Later versions allow us to specify what we already own, a rack ID, etc.
metadata(Topics) ->
    #{
        topics => Topics, user_data => <<>>
    }.

-spec assign(
    Members :: [kafine_assignor:member()],
    TopicPartitions :: kafine_topic_partitions:t(),
    AssignmentUserData :: binary()
) -> kafine_assignor:assignments().

%% Given a list of members and topics/partitions, assign the partitions to the members.
%% This assignor aims to co-locate partitions of several topics.
%% That is: partition 0 of each topic should go to a single member, partition 1 of
%% each topic should go to a single (possibly different) member.
%% C1 :: (TA, P0), (TB, P0), (TA, P1), (TB, P1)
%% C2 :: (TA, P2), (TB, P2)
assign(Members0, TopicPartitions, _AssignmentUserData) ->
    % Members are sorted before we start.
    Members = lists:sort(Members0),
    Empty = create_empty_assignments(Members),
    do_assign(Members, TopicPartitions, Empty).

%% To make life easier in 'assign_to', start every member with an empty assignment.
-spec create_empty_assignments(Members :: [kafine_assignor:member()]) ->
    kafine_assignor:assignments().

create_empty_assignments(Members) ->
    Initial = #{assigned_partitions => #{}, user_data => <<>>},
    create_initial_assignments(Members, Initial).

-spec create_initial_assignments(
    Members :: [kafine_assignor:member()],
    Initial :: kafine_assignor:member_assignment()
) ->
    kafine_assignor:assignments().

create_initial_assignments(Members, Initial) ->
    lists:foldl(
        fun(#{member_id := MemberId}, Acc) ->
            Acc#{MemberId => Initial}
        end,
        #{},
        Members
    ).
%% For each topic, share the partitions between the members.
do_assign(Members0, TopicPartitions, Acc0) ->
    maps:fold(
        fun(Topic, Partitions, Acc1) ->
            % Only include the members that want this topic.
            Members = lists:filter(
                fun(_M = #{metadata := #{topics := Topics}}) ->
                    lists:member(Topic, Topics)
                end,
                Members0
            ),
            MemberCount = length(Members),
            Shares = share(Partitions, MemberCount),
            assign_to(Topic, lists:zip(Members, Shares), Acc1)
        end,
        Acc0,
        TopicPartitions
    ).

assign_to(Topic, [{_Member = #{member_id := MemberId}, Share} | Rest], Acc) ->
    Acc2 = maps:update_with(
        MemberId,
        fun(MemberAssignment = #{assigned_partitions := AssignedPartitions}) ->
            MemberAssignment#{assigned_partitions := AssignedPartitions#{Topic => Share}}
        end,
        Acc
    ),
    assign_to(Topic, Rest, Acc2);
assign_to(_Topic, [], Acc) ->
    Acc.

% Given a list of items, divide it into N lists of items. The resulting lists should try to be the same length.
% Extra items are given to the first lists in the result.
% Note that this is NOT equivalent to Elixir's Enum.chunk_every; see the comment below.
-spec share([Element], Shares :: non_neg_integer()) -> [[Element]].

share(List, N) when is_list(List), N > 0 ->
    L = length(List),
    % How many items in each list, minimum?
    D = L div N,
    % How many left over?
    R = L rem N,

    lists:reverse(share(List, N, D, R, [])).

share([], _N = 0, _D, _R, Acc) ->
    Acc;
share(List, N, D, R, Acc) when R > 0 ->
    % While we have spares, take an extra one.
    {Take, Rest} = lists:split(D + 1, List),
    share(Rest, N - 1, D, R - 1, [Take | Acc]);
share(List, N, D, R = 0, Acc) ->
    % No spares; take exact amount.
    {Take, Rest} = lists:split(D, List),
    share(Rest, N - 1, D, R, [Take | Acc]).

-ifdef(TEST).
share_test() ->
    % With only one member, it should get everything.
    ?assertEqual([[1, 2]], share([1, 2], 1)),
    ?assertEqual([[1, 2, 3]], share([1, 2, 3], 1)),

    % Spare items are allocated to the earlier lists first.
    ?assertEqual([[1], [2]], share([1, 2], 2)),
    ?assertEqual([[1, 2], [3]], share([1, 2, 3], 2)),

    % If there are not enough items, we expect some empty lists.
    ?assertEqual([[1], [2], []], share([1, 2], 3)),
    ?assertEqual([[1], [2], [], []], share([1, 2], 4)),
    ?assertEqual([[1], [2], [3], []], share([1, 2, 3], 4)),

    ?assertEqual([[1], [2], [3]], share([1, 2, 3], 3)),

    % More examples of too many items; those should be given (one at a time) to the earlier lists first.
    ?assertEqual([[1, 2], [3, 4], [5, 6]], share(lists:seq(1, 6), 3)),
    ?assertEqual([[1, 2, 3], [4, 5], [6, 7]], share(lists:seq(1, 7), 3)),
    ?assertEqual([[1, 2, 3], [4, 5, 6], [7, 8]], share(lists:seq(1, 8), 3)),
    ?assertEqual([[1, 2, 3], [4, 5, 6], [7, 8, 9]], share(lists:seq(1, 9), 3)),

    % Note that Elixir's Enum.chunk_every/2 would do the following, which is not what we want:
    % assert Enum.chunk_every(1..7, div(7, 3) + 1) == [[1, 2, 3], [4, 5, 6], [7]]
    ok.

-endif.
