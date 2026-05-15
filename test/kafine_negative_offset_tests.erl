-module(kafine_negative_offset_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("assert_meck.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(CONNECTION_OPTIONS, #{}).
-define(CONSUMER_OPTIONS, #{}).
-define(SUBSCRIBER_OPTIONS, #{}).
-define(FETCHER_METADATA, #{}).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun against_empty_non_zero_offset_topic/0
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(kafine),

    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, handle_partition_data, fun(_T, _P, _PD, St) ->
        {ok, St}
    end),
    ok.

cleanup(_) ->
    meck:unload(),
    application:stop(kafine),
    ok.

against_empty_non_zero_offset_topic() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % a topic with a single partition.
    Topic = ?TOPIC_NAME,
    meck:expect(
        kamock_metadata,
        handle_metadata_request,
        kamock_metadata:with_topics([Topic])
    ),

    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        kamock_metadata_response_topic:partitions(#{Topic => 1})
    ),

    % which is empty and has non-zero offsets.
    meck:expect(
        kamock_list_offsets_partition_response,
        make_list_offsets_partition_response,
        kamock_list_offsets_partition_response:range(11662, 11662)
    ),

    MessageBuilder = fun(_Topic, Partition, Offset) ->
        Key = iolist_to_binary(io_lib:format("key-~B-~B", [Partition, Offset])),
        Value = iolist_to_binary(io_lib:format("value-~B-~B", [Partition, Offset])),
        #{key => Key, value => Value}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:range(11662, 11662, MessageBuilder)
    ),

    % start the consumer.
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?SUBSCRIBER_OPTIONS,
        #{
            callback_mod => test_consumer_callback,
            callback_arg => undefined,
            skip_empty_fetches => after_first
        },
        [Topic],
        #{Topic => #{initial_offset => -1, offset_reset_policy => latest}},
        ?FETCHER_METADATA
    ),

    ?assertWait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(0)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Produce a message...
    meck:expect(
        kamock_list_offsets_partition_response,
        make_list_offsets_partition_response,
        kamock_list_offsets_partition_response:range(11662, 11663)
    ),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:range(11662, 11663, MessageBuilder)
    ),

    % ... which we should receive:
    ?assertWait(
        test_consumer_callback,
        handle_partition_data,
        ['_', '_', meck:is(has_message_count(1)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine:stop_topic_consumer(?CONSUMER_REF),
    kamock_broker:stop(Broker),
    ok.

has_message_count(ExpectedCount) ->
    fun(PartitionData) ->
        message_count(PartitionData) == ExpectedCount
    end.

message_count(PartitionData) ->
    {Records, _} = kafine_partition_data:flatten(PartitionData),
    length(Records).
