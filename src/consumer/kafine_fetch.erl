-module(kafine_fetch).

-export([
    build_request/2,
    handle_response/6
]).

-include_lib("kernel/include/logger.hrl").
-include_lib("kafcod/include/isolation_level.hrl").
-include_lib("kafcod/include/error_code.hrl").

-spec build_request(
    FetchInfo :: kafine_topic_partition_data:t({kafine:offset(), module(), any()}),
    kafine:consumer_options()
) -> fetch_request:fetch_request_11().

build_request(
    FetchInfo,
    Options = #{
        max_wait_ms := MaxWaitMs,
        min_bytes := MinBytes,
        max_bytes := MaxBytes,
        isolation_level := IsolationLevel0
    }
) ->
    IsolationLevel = isolation_level(IsolationLevel0),

    #{
        % We're a client, not a broker.
        replica_id => -1,

        max_wait_ms => MaxWaitMs,
        min_bytes => MinBytes,
        max_bytes => MaxBytes,

        isolation_level => IsolationLevel,

        % Sessions and forgotten topics are for inter-broker replication.
        session_id => 0,
        session_epoch => -1,
        forgotten_topics_data => [],

        topics => build_fetch_topics(FetchInfo, Options),

        % TODO: preferred read replica.
        rack_id => <<>>
    }.

isolation_level(read_committed) ->
    ?READ_COMMITTED;
isolation_level(read_uncommitted) ->
    ?READ_UNCOMMITTED.

build_fetch_topics(FetchInfo, #{partition_max_bytes := PartitionMaxBytes}) ->
    kafine_topic_partition_lists:from_topic_partition_data(
        topic,
        fun(_Topic, Partition, {Offset, _, _}) ->
            #{
                partition => Partition,
                fetch_offset => Offset,
                partition_max_bytes => PartitionMaxBytes,
                % Only used by brokers following the leader; not us.
                log_start_offset => -1,
                % TODO: We don't care about the leader epoch. At some point we might.
                current_leader_epoch => -1
            }
        end,
        FetchInfo
    ).

-spec handle_response(
    FetchResponse :: fetch_response:fetch_response_11(),
    FetchInfo :: kafine_topic_partition_data:t({kafine:offset(), module(), any()}),
    JobId :: kafine_fetcher:job_id(),
    NodeId :: kafine:node_id(),
    TopicOptions :: #{kafine:topic() => kafine:topic_options()},
    Owner :: pid()
) -> ok.

handle_response(
    #{
        error_code := ?NONE,
        responses := Responses,
        throttle_time_ms := _,
        session_id := _
    },
    FetchInfo,
    JobId,
    NodeId,
    TopicOptions,
    Owner
) ->
    Result = kafine_topic_partition_lists:to_topic_partition_data(
        topic,
        fun(Topic, PartitionData = #{partition_index := Partition}) ->
            case handle_partition_response(Topic, PartitionData, FetchInfo, TopicOptions) of
                {error, _} -> false;
                Result -> {Partition, Result}
            end
        end,
        Responses
    ),
    kafine_fetcher:complete_job(Owner, JobId, NodeId, Result).

handle_partition_response(
    Topic,
    PartitionData = #{
        partition_index := Partition,
        error_code := ?NONE,
        records := _,
        log_start_offset := _,
        last_stable_offset := _,
        high_watermark := _HighWatermark,
        aborted_transactions := _,
        preferred_read_replica := _
    },
    FetchInfo,
    _TopicOptions
) ->
    case kafine_topic_partition_data:get(Topic, Partition, FetchInfo, undefined) of
        {FetchOffset, CallbackMod, CallbackArgs} ->
            invoke_callback(CallbackMod, CallbackArgs, Topic, PartitionData, FetchOffset);
        undefined ->
            ?LOG_WARNING("Received fetch data for unrequested topic/partition ~s/~B", [
                Topic, Partition
            ]),
            {error, not_requested}
    end;
handle_partition_response(
    Topic,
    #{error_code := ?OFFSET_OUT_OF_RANGE},
    _FetchInfo,
    TopicOptions
) ->
    #{offset_reset_policy := OffsetResetPolicy} = maps:get(Topic, TopicOptions),
    {update_offset, OffsetResetPolicy};
handle_partition_response(
    _Topic,
    #{error_code := ?NOT_LEADER_OR_FOLLOWER},
    _FetchInfo,
    _TopicOptions
) ->
    give_away.

invoke_callback(
    CallbackMod, CallbackArgs, Topic, PartitionData = #{partition_index := Partition}, FetchOffset
) ->
    try
        case
            CallbackMod:handle_partition_data(
                CallbackArgs, Topic, PartitionData, FetchOffset, undefined
            )
        of
            ok ->
                completed;
            repeat ->
                repeat;
            BadReturn ->
                ?LOG_WARNING("~s/~B: Bad return from callback:  ~p", [
                    Topic, Partition, CallbackMod, BadReturn
                ]),
                {error, {bad_return_value, BadReturn}}
        end
    catch
        Class:Reason:StackTrace ->
            ?LOG_WARNING("~s/~B: Error in callback module ~p. ~p ~p ~p", [
                Topic, Partition, CallbackMod, Class, Reason, StackTrace
            ]),
            {error, {Class, Reason}}
    end.
