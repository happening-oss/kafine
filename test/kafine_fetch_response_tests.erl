-module(kafine_fetch_response_tests).
% Used by other tests.
-export([
    init_topic_partition_states/1,
    cleanup_topic_partition_states/1
]).

-include_lib("eunit/include/eunit.hrl").
-include("assert_meck.hrl").
% -include("kafine_tests.hrl").

-include_lib("kafcod/include/error_code.hrl").

-include("src/consumer/kafine_topic_partition_state.hrl").

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION, 61).
-define(TELEMETRY_EVENT_METADATA, #{node_id => 501}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun empty_response_leaves_offset_unchanged/0,
        fun unwanted_records_are_dropped/0,
        fun unwanted_records_are_dropped_2/0,
        fun unsubscribed/0
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    meck:expect(kafine_consumer, init_ack, fun(_Ref, _Topic, _Partition, _State) -> ok end),
    meck:expect(kafine_consumer, continue, fun(
        _Ref, _Topic, _Partition, _NextOffset, _NextState, _Span
    ) ->
        ok
    end),
    meck:expect(kafine_consumer, unsubscribe, fun(_Ref, _TopicPartitions) ->
        ok
    end),
    ok.

cleanup(_) ->
    meck:unload().

empty_response_leaves_offset_unchanged() ->
    TopicName = ?TOPIC_NAME,
    EmptyFetchResponse = #{
        error_code => ?NONE,
        responses => [
            #{
                topic => TopicName,
                partitions => [
                    #{
                        partition_index => ?PARTITION,
                        error_code => ?NONE,
                        % An empty fetch response returns empty 'records' here.
                        % See 'fetch_response_tests:v11_empty_response_test/0' in kafcod.
                        records => [],
                        high_watermark => 0,
                        last_stable_offset => 0,
                        log_start_offset => 0,
                        aborted_transactions => [],
                        preferred_read_replica => -1
                    }
                ]
            }
        ],
        throttle_time_ms => 0,
        session_id => 0
    },

    TopicPartitionStates = init_topic_partition_states(
        #{
            TopicName => #{
                ?PARTITION => #{}
            }
        }
    ),

    {TopicPartitionStates2, Errors} =
        kafine_fetch_response:fold(
            EmptyFetchResponse,
            TopicPartitionStates,
            ?TELEMETRY_EVENT_METADATA
        ),
    ?assertEqual(#{}, Errors),

    % Assert that we've been marked as busy.
    ?assertEqual(make_busy(TopicPartitionStates), TopicPartitionStates2),

    % Wait until continue is called.
    meck:wait(kafine_consumer, continue, '_', ?WAIT_TIMEOUT_MS),

    % No records => no change
    ?assertCalled(kafine_consumer, continue, ['_', ?TOPIC_NAME, ?PARTITION, 0, active, '_']),

    cleanup_topic_partition_states(TopicPartitionStates),
    ok.

unwanted_records_are_dropped() ->
    % If a Produce request contains multiple messages, these will usually end up in a single batch, so the batch might
    % end up containing messages with offsets [41, 42, 43, 44, ...].
    %
    % If you then send a Fetch request for offset 43, the Fetch response will include the entire batch.
    %
    % kafine_fetch_response is responsible for skipping over those messages.

    TopicName = ?TOPIC_NAME,

    % Unix epoch, milliseconds; 2024-08-14T17:41:14.686Z
    Timestamp = 1723657274686,
    _FormattedTimestamp = calendar:system_time_to_rfc3339(
        Timestamp, [{unit, millisecond}, {offset, "Z"}]
    ),

    MessageCount = 4,
    BaseOffset = 41,
    % LastOffset is exclusive; if we have [41, 42, 43, 44], it should be 45.
    LastOffset = BaseOffset + MessageCount,

    LastOffsetDelta = LastOffset - BaseOffset - 1,
    Records = [
        % All records in one batch.
        make_record(BaseOffset, OffsetDelta)
     || OffsetDelta <- lists:seq(0, LastOffsetDelta)
    ],
    ?assertEqual(MessageCount, length(Records)),
    RecordBatches = [
        make_record_batch(BaseOffset, LastOffsetDelta, Timestamp, Records)
    ],
    PartitionData = make_partition_data(?PARTITION, RecordBatches, LastOffset),
    FetchResponse = #{
        error_code => ?NONE,
        responses => [
            #{
                topic => TopicName,
                partitions => [PartitionData]
            }
        ],
        throttle_time_ms => 0,
        session_id => 0
    },

    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION => #{offset => 43}
        }
    }),

    {_TopicPartitionStates2, Errors} = kafine_fetch_response:fold(
        FetchResponse, TopicPartitionStates, ?TELEMETRY_EVENT_METADATA
    ),

    % Wait until continue is called.
    meck:wait(kafine_consumer, continue, '_', ?WAIT_TIMEOUT_MS),

    ?assertEqual(#{}, Errors),
    ?assertMatch(
        [
            {_, {_, init, [TopicName, ?PARTITION, ?CALLBACK_ARGS]}, {ok, ?CALLBACK_STATE}},
            {_, {_, begin_record_batch, [TopicName, ?PARTITION, 43, _, _]}, {ok, _}},
            % 41 and 42 should be dropped; we should see 43 and 44.
            {_, {_, handle_record, [TopicName, ?PARTITION, #{key := <<"key43">>}, _]}, {ok, _}},
            {_, {_, handle_record, [TopicName, ?PARTITION, #{key := <<"key44">>}, _]}, {ok, _}},
            % end_record_batch should see the offset of the _next_ record, i.e. 45.
            {_, {_, end_record_batch, [TopicName, ?PARTITION, 45, _, _]}, {ok, _}}
        ],
        meck:history(test_consumer_callback)
    ),

    ?assertCalled(kafine_consumer, continue, ['_', ?TOPIC_NAME, ?PARTITION, 45, active, '_']),

    cleanup_topic_partition_states(TopicPartitionStates),
    ok.

unwanted_records_are_dropped_2() ->
    % This is a variant of the above test where the broker has given us two batches, but our fetch offset is in the
    % second batch. I don't think the broker _will_ ever do this, but I spotted a bug in kafine_fetch_response that
    % might get bitten if it does, so here's a regression test.

    TopicName = ?TOPIC_NAME,

    RecordBatches = [
        make_record_batch(51, 54),
        make_record_batch(54, 57)
    ],

    PartitionData = make_partition_data(?PARTITION, RecordBatches, 57),
    FetchResponse = #{
        error_code => ?NONE,
        responses => [
            #{
                topic => TopicName,
                partitions => [PartitionData]
            }
        ],
        throttle_time_ms => 0,
        session_id => 0
    },

    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION => #{offset => 55}
        }
    }),

    {_TopicPartitionStates2, Errors} = kafine_fetch_response:fold(
        FetchResponse, TopicPartitionStates, ?TELEMETRY_EVENT_METADATA
    ),

    % Wait until continue is called.
    meck:wait(kafine_consumer, continue, '_', ?WAIT_TIMEOUT_MS),

    ?assertEqual(#{}, Errors),
    ?assertMatch(
        [
            {_, {_, init, [TopicName, ?PARTITION, ?CALLBACK_ARGS]}, {ok, ?CALLBACK_STATE}},
            {_, {_, begin_record_batch, [TopicName, ?PARTITION, 55, _, _]}, {ok, _}},
            % 51, 52, 53, 54 should be dropped; we should start at 55.
            {_, {_, handle_record, [TopicName, ?PARTITION, #{key := <<"key55">>}, _]}, {ok, _}},
            {_, {_, handle_record, [TopicName, ?PARTITION, #{key := <<"key56">>}, _]}, {ok, _}},
            {_, {_, end_record_batch, [TopicName, ?PARTITION, 57, _, _]}, {ok, _}}
        ],
        meck:history(test_consumer_callback)
    ),

    ?assertCalled(kafine_consumer, continue, ['_', ?TOPIC_NAME, ?PARTITION, 57, active, '_']),

    cleanup_topic_partition_states(TopicPartitionStates),
    ok.

unsubscribed() ->
    TopicName = ?TOPIC_NAME,
    EmptyFetchResponse = #{
        error_code => ?NONE,
        responses => [
            #{
                topic => TopicName,
                partitions => [
                    #{
                        partition_index => ?PARTITION,
                        error_code => ?NONE,
                        % An empty fetch response returns empty 'records' here.
                        % See 'fetch_response_tests:v11_empty_response_test/0' in kafcod.
                        records => [],
                        high_watermark => 0,
                        last_stable_offset => 0,
                        log_start_offset => 0,
                        aborted_transactions => [],
                        preferred_read_replica => -1
                    }
                ]
            }
        ],
        throttle_time_ms => 0,
        session_id => 0
    },

    % We unsubscribed from the topic/partition while it was in flight...
    TopicPartitionStates = #{},
    {TopicPartitionStates2, Errors} =
        kafine_fetch_response:fold(
            EmptyFetchResponse,
            TopicPartitionStates,
            ?TELEMETRY_EVENT_METADATA
        ),

    % No records => no change
    ?assertEqual(TopicPartitionStates, TopicPartitionStates2),
    ?assertEqual(#{}, Errors),
    ok.

init_topic_partition_states(InitStates) ->
    maps:map(
        fun(TopicName, InitPartitionStates) ->
            maps:map(
                fun(PartitionIndex, InitPartitionState) ->
                    InitState = maps:get(state, InitPartitionState, active),
                    InitOffset = maps:get(offset, InitPartitionState, 0),
                    {ok, ClientPid} = kafine_consumer_callback_process:start_link(
                        ?CONSUMER_REF,
                        TopicName,
                        PartitionIndex,
                        test_consumer_callback,
                        ?CALLBACK_ARGS
                    ),
                    #topic_partition_state{
                        state = InitState,
                        offset = InitOffset,
                        client_pid = ClientPid
                    }
                end,
                InitPartitionStates
            )
        end,
        InitStates
    ).

cleanup_topic_partition_states(TopicPartitionStates) ->
    maps:foreach(
        fun(_TopicName, PartitionStates) ->
            maps:foreach(
                fun(_PartitionIndex, #topic_partition_state{client_pid = ClientPid}) ->
                    kafine_consumer_callback_process:stop(ClientPid)
                end,
                PartitionStates
            )
        end,
        TopicPartitionStates
    ).

make_partition_data(PartitionIndex, RecordBatches, LastOffset) ->
    #{
        partition_index => PartitionIndex,
        error_code => ?NONE,
        records => RecordBatches,
        high_watermark => LastOffset,
        last_stable_offset => LastOffset,
        log_start_offset => 0,
        aborted_transactions => [],
        preferred_read_replica => -1
    }.

make_record_batch(BaseOffset, LastOffsetDelta, Timestamp, Records) ->
    #{
        base_offset => BaseOffset,
        partition_leader_epoch => 0,
        magic => 2,
        crc => -1,
        attributes => #{compression => none},
        last_offset_delta => LastOffsetDelta,
        base_timestamp => Timestamp,
        max_timestamp => Timestamp,
        producer_id => -1,
        producer_epoch => -1,
        base_sequence => -1,
        records => Records
    }.

make_record(BaseOffset, OffsetDelta) ->
    Offset = BaseOffset + OffsetDelta,
    Key = iolist_to_binary(io_lib:format("key~B", [Offset])),
    Value = iolist_to_binary(io_lib:format("value~B", [Offset])),
    Headers = [],
    #{
        attributes => 0,
        key => Key,
        value => Value,
        headers => Headers,
        offset_delta => OffsetDelta,
        timestamp_delta => 0
    }.

make_record_batch(BeginOffset, EndOffset) ->
    % BeginOffset is inclusive, EndOffset is exclusive.

    % Unix epoch, milliseconds; 2024-08-14T17:41:14.686Z
    Timestamp = 1723657274686,
    _FormattedTimestamp = calendar:system_time_to_rfc3339(
        Timestamp, [{unit, millisecond}, {offset, "Z"}]
    ),

    LastOffsetDelta = EndOffset - BeginOffset - 1,
    Records = [
        make_record(BeginOffset, OffsetDelta)
     || OffsetDelta <- lists:seq(0, LastOffsetDelta)
    ],

    make_record_batch(BeginOffset, LastOffsetDelta, Timestamp, Records).

make_busy(TopicPartitionStates) ->
    maps:map(
        fun(_Topic, PartitionStates) ->
            maps:map(
                fun(_Partition, PartitionState) ->
                    PartitionState#topic_partition_state{state = busy}
                end,
                PartitionStates
            )
        end,
        TopicPartitionStates
    ).
