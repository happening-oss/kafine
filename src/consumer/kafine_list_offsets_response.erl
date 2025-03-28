-module(kafine_list_offsets_response).
-moduledoc false.
-export([fold/2]).

-include_lib("kafcod/include/error_code.hrl").
-include("kafine_topic_partition_state.hrl").

fold(
    _ListOffsetsResponse = #{topics := TopicOffsets},
    TopicPartitionStates
) ->
    lists:foldl(
        fun(_Topic = #{name := TopicName, partitions := Partitions}, Acc1) ->
            lists:foldl(
                fun(
                    _Partition = #{
                        partition_index := PartitionIndex, error_code := ?NONE, offset := Offset
                    },
                    Acc2
                ) ->
                    update_offset(TopicName, PartitionIndex, Offset, Acc2)
                end,
                Acc1,
                Partitions
            )
        end,
        TopicPartitionStates,
        TopicOffsets
    ).

update_offset(TopicName, PartitionIndex, NextOffset, TopicPartitionStates) ->
    update_offset(
        TopicName,
        PartitionIndex,
        NextOffset,
        TopicPartitionStates,
        kafine_maps:get([TopicName, PartitionIndex], TopicPartitionStates, undefined)
    ).

update_offset(
    _TopicName,
    _PartitionIndex,
    _NextOffset,
    TopicPartitionStates,
    undefined
) ->
    % Not found; we probably unsubscribed while the request was in flight; leave it alone.
    TopicPartitionStates;
update_offset(
    TopicName,
    PartitionIndex,
    NextOffset0,
    TopicPartitionStates,
    TopicPartitionState = #topic_partition_state{offset = Offset}
) when Offset < 0 ->
    % We requested a negative offset; we got back 'latest'; it needs to be adjusted.
    NextOffset =
        case NextOffset0 + Offset of
            O when O >= 0 -> O;
            _ -> 0
        end,
    kafine_maps:put(
        [TopicName, PartitionIndex],
        TopicPartitionState#topic_partition_state{offset = NextOffset},
        TopicPartitionStates
    );
update_offset(
    TopicName,
    PartitionIndex,
    NextOffset,
    TopicPartitionStates,
    TopicPartitionState = #topic_partition_state{}
) ->
    kafine_maps:put(
        [TopicName, PartitionIndex],
        TopicPartitionState#topic_partition_state{offset = NextOffset},
        TopicPartitionStates
    ).
