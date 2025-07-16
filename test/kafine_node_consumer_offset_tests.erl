-module(kafine_node_consumer_offset_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun reset_offsets_requests_are_combined/0
    ]}.

setup() ->
    kafine_node_consumer_tests:setup(?MODULE).

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

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

start_node_consumer(Ref, Broker, TopicPartitionStates) ->
    kafine_node_consumer_tests:start_node_consumer(Ref, Broker, TopicPartitionStates).

stop_node_consumer(Pid, TopicPartitionStates) ->
    kafine_node_consumer_tests:stop_node_consumer(Pid, TopicPartitionStates).
