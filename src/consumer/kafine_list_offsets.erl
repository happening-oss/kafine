-module(kafine_list_offsets).
-moduledoc false.
-export([
    build_request/2,
    handle_response/5
]).

-include_lib("kafcod/include/timestamp.hrl").
-include_lib("kafcod/include/isolation_level.hrl").
-include_lib("kafcod/include/error_code.hrl").

%% Build a ListOffsets request.
%%
%% Given a map of Topic => Partition => Timestamp, build the corresponding request object.
-spec build_request(
    TopicPartitionTimestamps :: kafine_topic_partition_data:t(kafine:offset_timestamp()),
    IsolationLevel :: kafine:isolation_level()
) -> list_offsets_request:list_offsets_request_5().

build_request(TopicPartitionTimestamps, IsolationLevel0) ->
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

build_list_offsets_topics(TopicPartitionTimestamps) ->
    kafine_topic_partition_lists:from_topic_partition_data(
        fun(_Topic, Partition, OffsetTimestamp) ->
            #{
                partition_index => Partition,
                timestamp => convert_timestamp(OffsetTimestamp),
                current_leader_epoch => -1
            }
        end,
        TopicPartitionTimestamps
    ).

convert_timestamp(earliest) ->
    ?EARLIEST_TIMESTAMP;
convert_timestamp(latest) ->
    ?LATEST_TIMESTAMP;
convert_timestamp(Offset) when Offset < 0 ->
    % It's a negative offset; we want to count backwards from the end.
    ?LATEST_TIMESTAMP.

-spec handle_response(
    ListOffsetsResponse :: list_offsets_response:list_offsets_response_5(),
    RequestedOffsets :: kafine_topic_partition_data:t(kafine:offset_timestamp()),
    JobId :: kafine_fetcher:job_id(),
    NodeId :: kafine:node_id(),
    Owner :: pid()
) -> ok.

%% Handle a ListOffsets response
handle_response(#{topics := TopicPartitionOffsetsList}, RequestedOffsets, JobId, NodeId, Owner) ->
    Result =
        kafine_topic_partition_lists:to_topic_partition_data(
            fun(Topic, #{partition_index := Partition, offset := Offset, error_code := ?NONE}) ->
                case kafine_topic_partition_data:get(Topic, Partition, RequestedOffsets) of
                    Replay when is_number(Replay) andalso Replay < 0 ->
                        % Replay is negative, add it to the latest offset to get the desired offset
                        ReplayOffset =
                            case Offset + Replay of
                                RO when RO < 0 -> 0;
                                RO -> RO
                            end,
                        {Partition, {update_offset, ReplayOffset}};
                    _ ->
                        {Partition, {update_offset, Offset}}
                end
            end,
            TopicPartitionOffsetsList
        ),

    kafine_fetcher:complete_job(Owner, JobId, NodeId, Result).
