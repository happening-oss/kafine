-module(kafine_fetch_response_tests).

-include_lib("eunit/include/eunit.hrl").
-include("assert_meck.hrl").
% -include("kafine_tests.hrl").

-include_lib("kafcod/include/error_code.hrl").

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION, 61).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(FETCHER_METADATA, #{}).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun empty_response_leaves_offset_unchanged/0,
        fun unwanted_records_are_dropped/0,
        fun unwanted_records_are_dropped_2/0,
        fun skipping_empty_fetches_result_in_repeat_and_no_callback/0,
        fun skipping_empty_after_first_enables_skipping_after_first_response/0
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, handle_partition_data, fun(_T, _P, _PD, St) ->
        {ok, St}
    end),

    meck:new(kafine_parallel_handler, [passthrough]),

    meck:new(kafine_fetcher, [stub_all]),
    meck:expect(kafine_fetcher, whereis, fun(_) -> self() end),

    ok.

cleanup(_) ->
    meck:unload().

empty_response_leaves_offset_unchanged() ->
    TopicName = ?TOPIC_NAME,

    {ok, Handler} = kafine_parallel_handler:start_link(
        ?CONSUMER_REF,
        {TopicName, ?PARTITION},
        0,
        #{
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS,
            skip_empty_fetches => false,
            error_mode => reset
        },
        ?FETCHER_METADATA
    ),

    FetchInfo = #{
        TopicName => #{
            ?PARTITION => {0, kafine_parallel_handler, {Handler, false}}
        }
    },

    PartitionData =
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
        },

    EmptyFetchResponse = #{
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

    {ok, Result} = kafine_fetch:handle_response(EmptyFetchResponse, FetchInfo),

    % Should complete job
    ?assertEqual(
        #{
            TopicName => #{
                ?PARTITION => completed
            }
        },
        Result
    ),

    % Validate for parallel handler call
    ?assertCalled(
        kafine_parallel_handler,
        handle_partition_data,
        [
            {Handler, false},
            TopicName,
            PartitionData,
            0,
            '_'
        ]
    ),

    % Should re-request the same offset
    ?assertWait(
        kafine_fetcher,
        fetch,
        [
            '_', TopicName, ?PARTITION, 0, kafine_parallel_handler, {Handler, false}
        ],
        ?WAIT_TIMEOUT_MS
    ),

    ok.

unwanted_records_are_dropped() ->
    % If a Produce request contains multiple messages, these will usually end up in a single batch, so the batch might
    % end up containing messages with offsets [41, 42, 43, 44, ...].
    %
    % If you then send a Fetch request for offset 43, the Fetch response will include the entire batch.
    %
    % kafine_fetch:handle_response is responsible for skipping over those messages.

    TopicName = ?TOPIC_NAME,
    StartOffset = 43,

    {ok, Handler} = kafine_parallel_handler:start_link(
        ?CONSUMER_REF,
        {TopicName, ?PARTITION},
        StartOffset,
        #{
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS,
            skip_empty_fetches => false,
            error_mode => reset
        },
        ?FETCHER_METADATA
    ),

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

    FetchInfo = #{
        TopicName => #{
            ?PARTITION => {StartOffset, kafine_parallel_handler, {Handler, false}}
        }
    },

    {ok, Result} = kafine_fetch:handle_response(FetchResponse, FetchInfo),

    ?assertEqual(
        #{
            TopicName => #{
                ?PARTITION => completed
            }
        },
        Result
    ),

    % Wait until next fetch is called.
    ?assertWait(
        kafine_fetcher,
        fetch,
        [
            '_', TopicName, ?PARTITION, 45, kafine_parallel_handler, {Handler, false}
        ],
        ?WAIT_TIMEOUT_MS
    ),

    % TODO: not sure this test adds anything now that the skipping is in handle_partition_data
    ?assertMatch(
        [
            {_, {_, init, [TopicName, ?PARTITION, ?CALLBACK_ARGS]}, {ok, ?CALLBACK_STATE}},
            {_, {_, handle_partition_data, [TopicName, ?PARTITION, _, _]}, {ok, _}}
        ],
        meck:history(test_consumer_callback)
    ),

    ok.

unwanted_records_are_dropped_2() ->
    % This is a variant of the above test where the broker has given us two batches, but our fetch offset is in the
    % second batch. I don't think the broker _will_ ever do this, but I spotted a bug in kafine_fetch_response that
    % might get bitten if it does, so here's a regression test.

    TopicName = ?TOPIC_NAME,
    StartOffset = 55,

    {ok, Handler} = kafine_parallel_handler:start_link(
        ?CONSUMER_REF,
        {TopicName, ?PARTITION},
        StartOffset,
        #{
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS,
            skip_empty_fetches => false,
            error_mode => reset
        },
        ?FETCHER_METADATA
    ),

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

    FetchInfo = #{
        TopicName => #{
            ?PARTITION => {55, kafine_parallel_handler, {Handler, false}}
        }
    },

    {ok, Result} = kafine_fetch:handle_response(FetchResponse, FetchInfo),

    ?assertEqual(
        #{
            TopicName => #{
                ?PARTITION => completed
            }
        },
        Result
    ),

    % Wait until next fetch is called.
    ?assertWait(
        kafine_fetcher,
        fetch,
        [
            '_', TopicName, ?PARTITION, 57, kafine_parallel_handler, {Handler, false}
        ],
        ?WAIT_TIMEOUT_MS
    ),

    % TODO: not sure this test adds anything now that the skipping is in handle_partition_data
    ?assertMatch(
        [
            {_, {_, init, [TopicName, ?PARTITION, ?CALLBACK_ARGS]}, {ok, ?CALLBACK_STATE}},
            {_, {_, handle_partition_data, [TopicName, ?PARTITION, _, _]}, {ok, _}}
        ],
        meck:history(test_consumer_callback)
    ),

    ok.

skipping_empty_fetches_result_in_repeat_and_no_callback() ->
    TopicName = ?TOPIC_NAME,

    {ok, Handler} = kafine_parallel_handler:start_link(
        ?CONSUMER_REF,
        {TopicName, ?PARTITION},
        0,
        #{
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS,
            skip_empty_fetches => true,
            error_mode => reset
        },
        ?FETCHER_METADATA
    ),
    meck:reset(kafine_fetcher),

    FetchInfo = #{
        TopicName => #{
            ?PARTITION => {0, kafine_parallel_handler, {Handler, true}}
        }
    },

    PartitionData =
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
        },

    EmptyFetchResponse = #{
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

    {ok, Result} = kafine_fetch:handle_response(EmptyFetchResponse, FetchInfo),

    ?assertEqual(
        #{
            TopicName => #{
                ?PARTITION => repeat
            }
        },
        Result
    ),

    % Verify for parallel handler call
    ?assertCalled(
        kafine_parallel_handler,
        handle_partition_data,
        [
            {Handler, true},
            TopicName,
            PartitionData,
            0,
            '_'
        ]
    ),

    ?assertMatch(
        [
            {_, {_, init, [TopicName, ?PARTITION, ?CALLBACK_ARGS]}, {ok, ?CALLBACK_STATE}}
        ],
        meck:history(test_consumer_callback)
    ),

    % Should not re-request the same offset
    ?assertNotCalled(kafine_fetcher, fetch, '_'),

    ok.

skipping_empty_after_first_enables_skipping_after_first_response() ->
    % If a Produce request contains multiple messages, these will usually end up in a single batch, so the batch might
    % end up containing messages with offsets [41, 42, 43, 44, ...].
    %
    % If you then send a Fetch request for offset 43, the Fetch response will include the entire batch.
    %
    % kafine_fetch:handle_response is responsible for skipping over those messages.

    TopicName = ?TOPIC_NAME,
    StartOffset = 43,

    {ok, Handler} = kafine_parallel_handler:start_link(
        ?CONSUMER_REF,
        {TopicName, ?PARTITION},
        StartOffset,
        #{
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS,
            skip_empty_fetches => after_first,
            error_mode => reset
        },
        ?FETCHER_METADATA
    ),

    ?assertWait(
        kafine_fetcher,
        fetch,
        [
            '_', TopicName, ?PARTITION, StartOffset, kafine_parallel_handler, {Handler, false}
        ],
        ?WAIT_TIMEOUT_MS
    ),

    % Unix epoch, milliseconds; 2024-08-14T17:41:14.686Z
    Timestamp = 1723657274686,
    _FormattedTimestamp = calendar:system_time_to_rfc3339(
        Timestamp, [{unit, millisecond}, {offset, "Z"}]
    ),

    MessageCount = 2,
    BaseOffset = 43,
    % LastOffset is exclusive; if we have [43, 44], it should be 45.
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

    FetchInfo = #{
        TopicName => #{
            ?PARTITION => {StartOffset, kafine_parallel_handler, {Handler, false}}
        }
    },

    {ok, Result} = kafine_fetch:handle_response(FetchResponse, FetchInfo),

    ?assertEqual(
        #{
            TopicName => #{
                ?PARTITION => completed
            }
        },
        Result
    ),

    % Wait until next fetch is called. SkipEmpty should be true
    ?assertWait(
        kafine_fetcher,
        fetch,
        [
            '_', TopicName, ?PARTITION, 45, kafine_parallel_handler, {Handler, true}
        ],
        ?WAIT_TIMEOUT_MS
    ),

    ?assertMatch(
        [
            {_, {_, init, [TopicName, ?PARTITION, ?CALLBACK_ARGS]}, {ok, ?CALLBACK_STATE}},
            {_, {_, handle_partition_data, [TopicName, ?PARTITION, _, _]}, {ok, _}}
        ],
        meck:history(test_consumer_callback)
    ),

    ok.

make_partition_data(PartitionIndex, RecordBatches, LastOffset) ->
    kamock_partition_data_builder:make_partition_data(PartitionIndex, 0, LastOffset, RecordBatches).

make_record_batch(BaseOffset, LastOffsetDelta, Timestamp, Records) ->
    kamock_partition_data_builder:make_record_batch(
        BaseOffset, LastOffsetDelta, Timestamp, Records
    ).

make_record(BaseOffset, OffsetDelta) ->
    Offset = BaseOffset + OffsetDelta,
    Message = #{
        key => iolist_to_binary(io_lib:format("key~B", [Offset])),
        value => iolist_to_binary(io_lib:format("value~B", [Offset])),
        headers => []
    },
    kamock_partition_data_builder:make_record(OffsetDelta, Message).

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
