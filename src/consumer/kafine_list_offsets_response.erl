-module(kafine_list_offsets_response).
-moduledoc false.
-export([fold/3]).

-include_lib("kafcod/include/error_code.hrl").
-include("kafine_topic_partition_state.hrl").

fold(
    _ListOffsetsResponse = #{topics := TopicOffsets},
    TopicPartitionStates,
    TopicOptions
) ->
    lists:foldl(
        fun(_Topic = #{name := TopicName, partitions := Partitions}, Acc1) ->
            #{TopicName := #{offset_reset_policy := OffsetResetPolicy}} = TopicOptions,
            lists:foldl(
                fun(
                    _Partition = #{
                        partition_index := PartitionIndex, error_code := ?NONE, offset := Offset
                    },
                    Acc2
                ) ->
                    update_offset(TopicName, PartitionIndex, Offset, OffsetResetPolicy, Acc2)
                end,
                Acc1,
                Partitions
            )
        end,
        TopicPartitionStates,
        TopicOffsets
    ).

update_offset(TopicName, PartitionIndex, Offset, OffsetResetPolicy, TopicPartitionStates) ->
    case kafine_maps:get([TopicName, PartitionIndex], TopicPartitionStates, undefined) of
        undefined ->
            TopicPartitionStates;
        TopicPartitionState = #topic_partition_state{offset = LastFetchedOffset} ->
            NextOffset = offset_from_policy(LastFetchedOffset, Offset, OffsetResetPolicy),
            kafine_maps:put(
                [TopicName, PartitionIndex],
                TopicPartitionState#topic_partition_state{offset = NextOffset},
                TopicPartitionStates
            )
    end.

offset_from_policy(_LastOffsetFetched, NextOffset, OffsetResetPolicy) when
    OffsetResetPolicy =:= latest; OffsetResetPolicy =:= earliest
->
    NextOffset;
offset_from_policy(LastOffsetFetched, NextOffset, OffsetResetPolicy) when
    is_atom(OffsetResetPolicy)
->
    OffsetResetPolicy:adjust_offset(LastOffsetFetched, NextOffset).
