-module(kafine_sticky_assignor).
-behaviour(kafine_assignor).

% A naive implementation of a sticky assignor. It attempts to preserve existing assignments. Uses the 'owned_partitions'
% of the members' subscription metadata.
%
% It attempts to keep the assignments balanced by (when needed) stealing excess partitions from some members to give to
% others.
%
% These stolen partitions are combined with any "orphaned" partitions and distributed among the remaining members using
% a weighted round-robin algorithm.
%
% It's "naive" because it's optimised for when all the members want the same topics. If this is not the case, it can get
% unbalanced. In particular, it steals partitions arbitrarily from an over-subscribed member. It won't be able to give
% them to another member that doesn't want them, and they might be re-assigned to the original member, so it remains
% over-subscribed.
%
% For example, it doesn't behave the same as Kafka's StickyAssignor when given Example 2 from
% https://kafka.apache.org/28/javadoc/org/apache/kafka/clients/consumer/StickyAssignor.html
%
% Note that this is essentially a bin-packing problem (thus NP-hard). There are reasonable (for small-N) algorithms for
% that. We should investigate them in future.
%
% Also note that this is an "eager" (stop the world) rebalancing assignor. It's not cooperative/incremental.

-export([
    name/0,
    assign/3
]).

name() -> <<"sticky">>.

-spec assign(
    Members :: [kafine_assignor:member()],
    ClusterMetadata :: kafine_cluster_metadata:t(),
    AssignmentUserData :: kafine_assignor:user_data()
) -> kafine_assignor:assignments().

assign(Members, ClusterMetadata, _AssignmentUserData) ->
    % Convert 'Members' into a form that's easier to work with.
    Working = convert_to_working(Members),

    % Calculate the orphaned partitions.
    TopicPartitions = flatten_topic_partitions(
        kafine_assignor:get_topic_partitions(Members, ClusterMetadata)
    ),
    OrphanedPartitions = get_orphaned_partitions(TopicPartitions, Working),

    % Work out how many partitions each member should have. Remove any excess partitions.
    %
    % Note: this naively assumes that every member wants the same topics; it'll easily get unbalanced otherwise.
    MemberCount = length(Members),
    PartitionCount = length(TopicPartitions),
    IdealPartitionCount = get_ideal_partition_count(PartitionCount, MemberCount),
    {Working2, StolenPartitions} = steal_excess_partitions(Working, IdealPartitionCount),

    % Assign the orphaned and stolen partitions.
    Working3 = assign_orphans(Working2, StolenPartitions ++ OrphanedPartitions),

    % Convert back from our working format.
    convert_from_working(Working3).

convert_to_working(Members) ->
    lists:map(
        fun(
            #{
                member_id := MemberId,
                metadata := #{
                    topics := Topics,
                    owned_partitions := OwnedPartitions
                }
            }
        ) ->
            {MemberId, Topics, [
                {Topic, Partition}
             || #{topic := Topic, partitions := Partitions} <:- OwnedPartitions,
                Partition <- Partitions
            ]}
        end,
        Members
    ).

convert_from_working(Working) ->
    #{
        MemberId => #{
            assigned_partitions => lists:foldl(
                fun({Topic, Partition}, Acc) ->
                    case Acc of
                        #{Topic := Partitions} ->
                            Acc#{Topic := [Partition | Partitions]};
                        #{} ->
                            Acc#{Topic => [Partition]}
                    end
                end,
                #{},
                OwnedPartitions
            ),
            user_data => <<>>
        }
     || {MemberId, _Topics, OwnedPartitions} <- Working
    }.

get_orphaned_partitions(TopicPartitions, Working) ->
    Owned = [
        {Topic, Partition}
     || {_MemberId, _Topics, OwnedPartitions} <:- Working, {Topic, Partition} <:- OwnedPartitions
    ],
    lists:sort(TopicPartitions -- Owned).

flatten_topic_partitions(TopicPartitions) ->
    [{Topic, Partition} || Topic := Partitions <:- TopicPartitions, Partition <- Partitions].

steal_excess_partitions(Members, IdealPartitionCount) ->
    lists:mapfoldl(
        fun(Member, StolenPartitions) ->
            steal_excess_partitions_from(Member, IdealPartitionCount, StolenPartitions)
        end,
        [],
        Members
    ).

steal_excess_partitions_from(
    Member = {_MemberId, _Topics, OwnedPartitions}, IdealPartitionCount, StolenPartitions
) when length(OwnedPartitions) =< IdealPartitionCount ->
    {Member, StolenPartitions};
steal_excess_partitions_from(
    _Member = {MemberId, Topics, OwnedPartitions}, IdealPartitionCount, StolenPartitions
) ->
    {OwnedPartitions2, Stolen} = lists:split(IdealPartitionCount, OwnedPartitions),
    Member2 = {MemberId, Topics, OwnedPartitions2},
    {Member2, StolenPartitions ++ Stolen}.

assign_orphans(Members, OrphanedPartitions) ->
    assign_orphans(lists:sort(fun by_partition_count/2, Members), [], OrphanedPartitions).

by_partition_count({_, _, X}, {_, _, Y}) ->
    % Members with fewer partitions should come first, because we want to fill them first.
    length(X) =< length(Y).

assign_orphans(Members = [_ | _], Seen, [Orphan | OrphanedPartitions]) ->
    assign_orphan(Members, Seen, Orphan, OrphanedPartitions);
assign_orphans(_Members = [], Seen, OrphanedPartitions) ->
    % Round-robin.
    assign_orphans(lists:reverse(Seen), OrphanedPartitions);
assign_orphans(Members, Seen, []) ->
    % No more partitions to assign; we're done here.
    Members ++ Seen.

assign_orphan(
    [Member = {MemberId, Topics, OwnedPartitions} | Members],
    Seen,
    TopicPartition = {Topic, _Partition},
    OrphanedPartitions
) ->
    case lists:member(Topic, Topics) of
        true ->
            % This member wants this topic; assign it and continue.
            Member2 = {MemberId, Topics, [TopicPartition | OwnedPartitions]},
            assign_orphans(Members ++ lists:reverse([Member2 | Seen]), OrphanedPartitions);
        false ->
            % This member doesn't want this topic; try the next one.
            assign_orphan(Members, [Member | Seen], TopicPartition, OrphanedPartitions)
    end;
assign_orphan([], Seen, _TopicPartition, OrphanedPartitions) ->
    % No member wants this topic; skip it.
    assign_orphans(lists:reverse(Seen), OrphanedPartitions).

get_ideal_partition_count(N, D) ->
    % Round up.
    (N + D - 1) div D.
