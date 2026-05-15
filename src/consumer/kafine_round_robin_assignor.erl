-module(kafine_round_robin_assignor).
-behaviour(kafine_assignor).

-export([
    name/0,
    assign/3
]).

name() -> <<"roundrobin">>.

-spec assign(
    Members :: [kafine_assignor:member()],
    ClusterMetadata :: kafine_cluster_metadata:t(),
    AssignmentUserData :: kafine_assignor:user_data()
) -> kafine_assignor:assignments().

assign(Members0, ClusterMetadata, _AssignmentUserData) ->
    % Members are sorted before we start.
    Members = lists:sort(Members0),
    TopicPartitions0 = kafine_assignor:get_topic_partitions(Members, ClusterMetadata),
    % Flatten TopicPartitions into [{Topic, Partition}, ...]
    TopicPartitions = flatten_topic_partitions(TopicPartitions0),
    Empty = create_empty_assignments(Members),
    kafine_round_robin:fold(fun assign_to/3, Empty, Members, TopicPartitions).

flatten_topic_partitions(TopicPartitions) ->
    % For consistency in the unit tests, we sort the tuples here. This results in 'assign_to' putting the partitions in
    % the reverse order, so we fix it up later.
    lists:sort(
        maps:fold(
            fun(Topic, Partitions, Acc) ->
                lists:foldl(
                    fun(Partition, Acc1) ->
                        [{Topic, Partition} | Acc1]
                    end,
                    Acc,
                    Partitions
                )
            end,
            [],
            TopicPartitions
        )
    ).

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

assign_to(
    _Member = #{member_id := MemberId, metadata := #{topics := Topics}},
    {Topic, Partition},
    Assignment
) ->
    case lists:member(Topic, Topics) of
        true ->
            Assignment2 = assign_to(MemberId, Topic, Partition, Assignment),
            {take, Assignment2};
        false ->
            skip
    end.

assign_to(MemberId, Topic, Partition, Assignment) ->
    maps:update_with(
        MemberId,
        fun(MemberAssignment = #{assigned_partitions := AssignedPartitions}) ->
            AssignedPartitions2 = maps:update_with(
                Topic,
                fun(Partitions) ->
                    % This results in the partitions being reversed in the topic assigned to the member. We
                    % sort them into ascending order later.
                    [Partition | Partitions]
                end,
                [Partition],
                AssignedPartitions
            ),
            MemberAssignment#{assigned_partitions := AssignedPartitions2}
        end,
        Assignment
    ).
