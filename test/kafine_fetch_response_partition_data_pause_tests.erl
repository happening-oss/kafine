-module(kafine_fetch_response_partition_data_pause_tests).
-include_lib("eunit/include/eunit.hrl").

-include("history_matchers.hrl").

-define(CALLBACK_STATE, {state, ?MODULE}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun single_batch_pause_offset_4/0,
        fun two_batches_pause_offset_4/0,

        fun single_batch_pause_end_record_batch/0,
        fun two_batches_pause_end_record_batch/0
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),
    ok.

cleanup(_) ->
    meck:unload().

single_batch_pause_offset_4() ->
    FetchOffset = 3,
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    % Pause after the message with offset 4. It's in the first batch. We shouldn't see messages 5-8.
    meck:expect(test_consumer_callback, handle_record, fun
        (_T, _P, _M = #{offset := 4}, St) -> {pause, St};
        (_T, _P, _M, St) -> {ok, St}
    end),

    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),

    Topic = <<"cars">>,
    Partition = 2,

    ?assertMatch(
        [
            ?begin_record_batch(Topic, Partition, FetchOffset, 0, 9, 9),
            ?handle_record(Topic, Partition, 3, _, _),
            ?handle_record(Topic, Partition, 4, _, _),
            ?end_record_batch(Topic, Partition, 5, 0, 9, 9)
        ],
        meck:history(test_consumer_callback)
    ),
    ?assertMatch({5, paused, ?CALLBACK_STATE}, FoldResult),
    ok.

two_batches_pause_offset_4() ->
    FetchOffset = 3,
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_two_batches(),
    {Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    % Pause after the message with offset 4. It's in the first batch. We shouldn't see messages 5-8.
    meck:expect(test_consumer_callback, handle_record, fun
        (_T, _P, _M = #{offset := 4}, St) -> {pause, St};
        (_T, _P, _M, St) -> {ok, St}
    end),

    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),

    Topic = <<"cars">>,
    Partition = 2,

    ?assertMatch(
        [
            ?begin_record_batch(Topic, Partition, FetchOffset, 0, 9, 9),
            ?handle_record(Topic, Partition, 3, _, _),
            ?handle_record(Topic, Partition, 4, _, _),
            ?end_record_batch(Topic, Partition, 5, 0, 9, 9)
        ],
        meck:history(test_consumer_callback)
    ),
    ?assertMatch({5, paused, ?CALLBACK_STATE}, FoldResult),
    ok.

single_batch_pause_end_record_batch() ->
    FetchOffset = 3,
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    % Pause at the end of the batch.
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _M, _Info, St) ->
        {pause, St}
    end),

    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),

    Topic = <<"cars">>,
    Partition = 2,

    ?assertMatch(
        [
            ?begin_record_batch(Topic, Partition, FetchOffset, 0, 9, 9),
            ?handle_record(Topic, Partition, 3, _, _),
            ?handle_record(Topic, Partition, 4, _, _),
            ?handle_record(Topic, Partition, 5, _, _),
            ?end_record_batch(Topic, Partition, 6, 0, 9, 9)
        ],
        meck:history(test_consumer_callback)
    ),
    ?assertMatch({6, paused, ?CALLBACK_STATE}, FoldResult),
    ok.

two_batches_pause_end_record_batch() ->
    FetchOffset = 3,
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_two_batches(),
    {Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    % Pause at the end of the batch.
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _M, _Info, St) ->
        {pause, St}
    end),

    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),

    Topic = <<"cars">>,
    Partition = 2,

    ?assertMatch(
        [
            ?begin_record_batch(Topic, Partition, FetchOffset, 0, 9, 9),
            ?handle_record(Topic, Partition, 3, _, _),
            ?handle_record(Topic, Partition, 4, _, _),
            ?handle_record(Topic, Partition, 5, _, _),
            ?handle_record(Topic, Partition, 6, _, _),
            ?handle_record(Topic, Partition, 7, _, _),
            ?handle_record(Topic, Partition, 8, _, _),
            ?end_record_batch(Topic, Partition, 9, 0, 9, 9)
        ],
        meck:history(test_consumer_callback)
    ),
    ?assertMatch({9, paused, ?CALLBACK_STATE}, FoldResult),
    ok.
