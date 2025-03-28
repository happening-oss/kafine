-module(kafine_node_consumer_offset_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun reset_offsets_requests_are_combined/0
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

    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload().

reset_offsets_requests_are_combined() ->
    % If we get a bunch of OFFSET_OUT_OF_RANGE errors, we want to avoid multiple round-trips and combine the ListOffsets
    % requests into one.
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{offset => 123},
            ?PARTITION_2 => #{offset => 123}
        }
    }),

    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % Wait until we get end_batch.
    [
        meck:wait(
            test_consumer_callback,
            end_record_batch,
            ['_', P, '_', '_', '_'],
            ?WAIT_TIMEOUT_MS
        )
     || P <- [?PARTITION_1, ?PARTITION_2]
    ],

    % There should only be one ListOffsets request.
    [{_, {_, handle_list_offsets_request, [ListOffsetsRequest, _]}, ListOffsetsResponse}] = meck:history(
        kamock_list_offsets
    ),
    % It should be for both partitions.
    ?assertMatch(
        #{topics := [#{name := TopicName, partitions := [_, _]}]},
        ListOffsetsRequest
    ),
    ?assertMatch(
        #{topics := [#{name := TopicName, partitions := [#{offset := 0}, #{offset := 0}]}]},
        ListOffsetsResponse
    ),

    kafine_node_consumer:stop(Pid),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

start_node_consumer(Ref, Broker, TopicPartitionStates) ->
    kafine_node_consumer_tests:start_node_consumer(Ref, Broker, TopicPartitionStates).

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

cleanup_topic_partition_states(TopicPartitionStates) ->
    kafine_fetch_response_tests:cleanup_topic_partition_states(TopicPartitionStates).
