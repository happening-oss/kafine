-module(kafine_metadata).
-export([
    group_by_leader/1,
    group_by_leader/3
]).

-include_lib("kafcod/include/error_code.hrl").

group_by_leader(Fun, Acc, TopicsMetadata) when is_function(Fun, 4) ->
    lists:foldl(
        fun(_Topic = #{name := TopicName, error_code := ?NONE, partitions := Partitions}, Acc1) ->
            lists:foldl(
                fun(
                    _Partition = #{
                        partition_index := PartitionIndex,
                        error_code := ?NONE,
                        leader_id := LeaderId
                    },
                    Acc2
                ) ->
                    Fun(LeaderId, TopicName, PartitionIndex, Acc2)
                end,
                Acc1,
                Partitions
            )
        end,
        Acc,
        TopicsMetadata
    ).

group_by_leader(TopicsMetadata) ->
    group_by_leader(
        fun(LeaderId, TopicName, PartitionIndex, Acc) ->
            kafine_maps:update_with(
                [LeaderId, TopicName],
                fun(PartitionIndexes) -> [PartitionIndex | PartitionIndexes] end,
                [PartitionIndex],
                Acc
            )
        end,
        #{},
        TopicsMetadata
    ).
