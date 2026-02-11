-module(kafine_topic_partition_lists).

-export([
    from_topic_partition_data/2,
    from_topic_partition_data/3,
    to_topic_partition_data/2,
    to_topic_partition_data/3,
    fold/3,
    fold/4
]).

-export_type([t/0]).

-type t() :: [#{partitions := [map()], atom() => any()}].

-spec from_topic_partition_data(
    Fun :: fun((Topic :: kafine:topic(), Partition :: kafine:partition(), DataType) -> map()),
    TopicPartitionData :: kafine_topic_partition_data:t(DataType)
) -> t() when
    DataType :: dynamic().

from_topic_partition_data(Fun, TopicPartitionData) ->
    from_topic_partition_data(name, Fun, TopicPartitionData).

-spec from_topic_partition_data(
    TopicKey :: atom(),
    Fun :: fun((Topic :: kafine:topic(), Partition :: kafine:partition(), DataType) -> map()),
    TopicPartitionData :: kafine_topic_partition_data:t(DataType)
) -> t() when
    DataType :: dynamic().

from_topic_partition_data(TopicKey, Fun, TopicPartitionData) ->
    maps:fold(
        fun(Topic, Partitions, TopicsAcc) ->
            [
                #{
                    TopicKey => Topic,
                    partitions => maps:fold(
                        fun(Partition, Data, PartitionsAcc) ->
                            [Fun(Topic, Partition, Data) | PartitionsAcc]
                        end,
                        [],
                        Partitions
                    )
                }
                | TopicsAcc
            ]
        end,
        [],
        TopicPartitionData
    ).

-spec to_topic_partition_data(
    Fun :: fun(
        (Topic :: kafine:topic(), PartitionData :: map()) -> {kafine:partition(), DataType} | false
    ),
    TopicPartitionList :: t()
) -> kafine_topic_partition_data:t(DataType) when
    DataType :: dynamic().

to_topic_partition_data(Fun, TopicPartitionList) ->
    to_topic_partition_data(name, Fun, TopicPartitionList).

-spec to_topic_partition_data(
    TopicKey :: atom(),
    Fun :: fun(
        (Topic :: kafine:topic(), PartitionData :: map()) -> {kafine:partition(), DataType} | false
    ),
    TopicPartitionList :: t()
) -> kafine_topic_partition_data:t(DataType) when
    DataType :: dynamic().

% Convert kafka-style #{name := Topic, partitions := [...]} lists into kafine_topic_partition_data
% Takes a fun(Topic, PartitionResult) which returns {PartitionIndex, Data}, or false to skip the
% topic-partition.
%
% Builds the map contents as {Key, Value}} lists first since this is much more efficient than
% repeatedly updating a map
to_topic_partition_data(TopicKey, Fun, TopicPartitionList) ->
    maps:from_list(
        lists:filtermap(
            fun(#{TopicKey := Topic, partitions := Partitions}) when is_binary(Topic) ->
                PartitionMap =
                    maps:from_list(
                        lists:filtermap(
                            fun(PartitionData) ->
                                case Fun(Topic, PartitionData) of
                                    false -> false;
                                    Result -> {true, Result}
                                end
                            end,
                            Partitions
                        )
                    ),
                case PartitionMap =:= #{} of
                    true -> false;
                    false -> {true, {Topic, PartitionMap}}
                end
            end,
            TopicPartitionList
        )
    ).

-spec fold(
    Fun :: fun((Topic :: kafine:topic(), PartitionData :: map(), Acc :: AccType) -> AccType),
    Acc :: AccType,
    TopicPartitionList :: t()
) -> AccType when
    AccType :: dynamic().

fold(Fun, Acc, TopicPartitionList) ->
    fold(name, Fun, Acc, TopicPartitionList).

-spec fold(
    TopicKey :: atom(),
    Fun :: fun((Topic :: kafine:topic(), PartitionData :: map(), Acc :: AccType) -> AccType),
    Acc :: AccType,
    TopicPartitionList :: t()
) -> AccType when
    AccType :: dynamic().

fold(TopicKey, Fun, Acc, TopicPartitionList) ->
    lists:foldl(
        fun(#{TopicKey := Topic, partitions := Partitions}, TopicsAcc) when is_binary(Topic) ->
            lists:foldl(
                fun(PartitionData, PartitionsAcc) ->
                    Fun(Topic, PartitionData, PartitionsAcc)
                end,
                TopicsAcc,
                Partitions
            )
        end,
        Acc,
        TopicPartitionList
    ).
