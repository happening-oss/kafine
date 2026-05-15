-module(kafine_fetch_response_partition_data_pause_tests).
-include_lib("eunit/include/eunit.hrl").

-define(CALLBACK_STATE, {state, ?MODULE}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun single_batch_pause_offset_4/0,
        fun two_batches_pause_offset_4/0,

        fun single_batch_pause_offset_5/0,
        fun two_batches_pause_offset_8/0
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, handle_partition_data, fun(_T, _P, _PD, St) ->
        {ok, St}
    end),
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
    meck:expect(test_consumer_callback, handle_partition_data, pause_at_offset(4)),

    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
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
    meck:expect(test_consumer_callback, handle_partition_data, pause_at_offset(4)),

    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),

    ?assertMatch({5, paused, ?CALLBACK_STATE}, FoldResult),
    ok.

single_batch_pause_offset_5() ->
    FetchOffset = 3,
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    % Pause at the end of the first batch.
    meck:expect(test_consumer_callback, handle_partition_data, pause_at_offset(5)),

    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),

    ?assertMatch({6, paused, ?CALLBACK_STATE}, FoldResult),
    ok.

two_batches_pause_offset_8() ->
    FetchOffset = 3,
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_two_batches(),
    {Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    % Pause at the end of the second batch.
    meck:expect(test_consumer_callback, handle_partition_data, pause_at_offset(8)),

    FoldResult = kafine_fetch_response_partition_data:fold(
        Topic,
        PartitionData,
        FetchOffset,
        test_consumer_callback,
        ?CALLBACK_STATE
    ),

    ?assertMatch({9, paused, ?CALLBACK_STATE}, FoldResult),
    ok.

pause_at_offset(ExpectedOffset) ->
    fun(_T, _P, PD, St) ->
        % We could just return {pause, NextOffset, State}, but we'll do it properly.
        {St2, NextOffset} = kafine_partition_data:reduce_while(
            fun
                (#{offset := Offset}, Acc) when Offset =:= ExpectedOffset -> {halt, Acc};
                (_, Acc) -> {cont, Acc}
            end,
            St,
            PD
        ),
        {pause, NextOffset, St2}
    end.
