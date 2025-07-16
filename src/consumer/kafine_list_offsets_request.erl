-module(kafine_list_offsets_request).
-moduledoc false.
-export([build_list_offsets_request/2]).

-include_lib("kafcod/include/timestamp.hrl").
-include_lib("kafcod/include/isolation_level.hrl").

%% Build a ListOffsets request.
%%
%% Given a map of Topic => Partition => Timestamp, build the corresponding request object.
-spec build_list_offsets_request(
    TopicPartitionTimestamps :: #{
        kafine:topic() => #{kafine:partition() => kafine:offset_timestamp()}
    },
    IsolationLevel :: kafine:isolation_level()
) -> list_offsets_request:list_offsets_request_5().

build_list_offsets_request(TopicPartitionTimestamps, IsolationLevel0) ->
    IsolationLevel = isolation_level(IsolationLevel0),
    #{
        topics => build_list_offsets_topics(TopicPartitionTimestamps),
        replica_id => -1,
        isolation_level => IsolationLevel
    }.

isolation_level(read_committed) ->
    ?READ_COMMITTED;
isolation_level(read_uncommitted) ->
    ?READ_UNCOMMITTED.

build_list_offsets_topics(TopicPartitions) ->
    maps:fold(
        fun(TopicName, PartitionTimestamps, Acc) ->
            [
                #{
                    name => TopicName,
                    partitions => [
                        #{
                            partition_index => PartitionIndex,
                            timestamp => convert_timestamp(Timestamp),
                            current_leader_epoch => -1
                        }
                     || PartitionIndex := Timestamp <- PartitionTimestamps
                    ]
                }
                | Acc
            ]
        end,
        [],
        TopicPartitions
    ).

convert_timestamp(earliest) ->
    ?EARLIEST_TIMESTAMP;
convert_timestamp(latest) ->
    ?LATEST_TIMESTAMP;
convert_timestamp(Offset) when Offset < 0 ->
    % It's a negative offset; we want to count backwards from the end.
    ?LATEST_TIMESTAMP.
