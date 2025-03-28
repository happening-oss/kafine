-module(kafine_metadata).
-moduledoc false.
-export([
    group_by_leader/1,
    fold/3
]).

-include_lib("kafcod/include/error_code.hrl").

group_by_leader(TopicsMetadata) ->
    fold(
        fun(
            TopicName,
            #{partition_index := PartitionIndex, error_code := ?NONE, leader_id := LeaderId},
            Acc
        ) ->
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

fold(Fun, Acc, TopicsMetadata) when is_function(Fun, 3) ->
    lists:foldl(
        fun(_Topic = #{name := TopicName, error_code := ?NONE, partitions := Partitions}, Acc1) ->
            lists:foldl(
                fun(Partition, Acc2) ->
                    Fun(TopicName, Partition, Acc2)
                end,
                Acc1,
                Partitions
            )
        end,
        Acc,
        TopicsMetadata
    ).
