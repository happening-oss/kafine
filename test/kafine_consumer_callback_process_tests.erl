-module(kafine_consumer_callback_process_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION, 61).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, undefined).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun state_updates/0
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
    ok.

cleanup(_) ->
    meck:unload().

state_updates() ->
    % Verify that the state is persisted. We just count the number of records we've seen. For flavour, we pause after
    % the first batch.
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, 0} end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St + 1} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) ->
        {pause, St}
    end),

    {ok, Pid} = kafine_consumer_callback_process:start_link(
        ?CONSUMER_REF, ?TOPIC_NAME, ?PARTITION, test_consumer_callback, ?CALLBACK_ARGS
    ),

    FetchOffset = 3,
    FetchResponse = kafine_fetch_response_partition_data_tests:canned_fetch_response_single_batch(),
    {_Topic, PartitionData} = kafine_fetch_response_partition_data_tests:split_fetch_response(
        FetchResponse
    ),

    {ok, {NextOffset, NextState}} = kafine_consumer_callback_process:partition_data(
        Pid, ?TOPIC_NAME, PartitionData, FetchOffset
    ),

    % There are three records in the batch (see canned_fetch_response_single_batch/0). If we started at FetchOffset = 3,
    % then NextOffset = 6.
    ?assertEqual(6, NextOffset),
    ?assertEqual(paused, NextState),
    % We assert that the callback state was updated (the records were counted). This couples us to the internal state of
    % kafine_consumer_callback_process, but I can live with that for now.
    ?assertEqual({state, test_consumer_callback, 3}, sys:get_state(Pid)),

    kafine_consumer_callback_process:stop(Pid),
    ok.
