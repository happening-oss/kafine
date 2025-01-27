-module(kafine_list_offsets_request).
-export([build_list_offsets_request/3]).

-include_lib("kafcod/include/timestamp.hrl").
-include_lib("kafcod/include/isolation_level.hrl").

-spec build_list_offsets_request(
    TopicPartitions :: #{kafine:topic() := [kafine:partition()]},
    TopicOptions :: #{kafine:topic() := kafine:topic_options()},
    IsolationLevel :: kafine:isolation_level()
) -> list_offsets_request:list_offsets_request_5().

build_list_offsets_request(TopicPartitions, TopicOptions, IsolationLevel0) ->
    IsolationLevel = isolation_level(IsolationLevel0),
    #{
        topics => build_list_offsets_topics(TopicPartitions, TopicOptions),
        replica_id => -1,
        isolation_level => IsolationLevel
    }.

isolation_level(read_committed) ->
    ?READ_COMMITTED;
isolation_level(read_uncommitted) ->
    ?READ_UNCOMMITTED.

build_list_offsets_topics(TopicPartitions, TopicOptions) ->
    maps:fold(
        fun(TopicName, Partitions, Acc) ->
            #{TopicName := #{offset_reset_policy := OffsetResetPolicy}} = TopicOptions,
            Timestamp = timestamp_from_offset_reset_policy(OffsetResetPolicy),
            [
                #{
                    name => TopicName,
                    partitions => [
                        #{
                            partition_index => PartitionIndex,
                            timestamp => Timestamp,
                            current_leader_epoch => -1
                        }
                     || PartitionIndex <- Partitions
                    ]
                }
                | Acc
            ]
        end,
        [],
        TopicPartitions
    ).

timestamp_from_offset_reset_policy(earliest) ->
    ?EARLIEST_TIMESTAMP;
timestamp_from_offset_reset_policy(latest) ->
    ?LATEST_TIMESTAMP;
timestamp_from_offset_reset_policy(OffsetResetPolicy) ->
    OffsetResetPolicy:timestamp().
