-module(kafine_consumer_negative_offset_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/timestamp.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(CALLBACK_ARGS, undefined).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun last_with_new_topic/0,
        fun last_with_single_message/0,
        fun last_with_multiple_messages/0
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

last_with_new_topic() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    % By default, there are no messages on the topic.
    TopicName = ?TOPIC_NAME,
    Partition = 0,
    Subscription = #{
        TopicName => {#{initial_offset => -1, offset_reset_policy => latest}, #{Partition => -1}}
    },

    kafine_consumer:subscribe(Consumer, Subscription),

    % We should see a ListOffsets, then a Fetch at zero.
    meck:wait(
        kamock_list_offsets,
        handle_list_offsets_request,
        [meck:is(has_timestamp(?LATEST_TIMESTAMP)), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(has_fetch_offset(0)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % If we produce a message, we should see it.
    kafine_kamock:produce(0, 1),

    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', '_', _NextOffset = 1, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            % We don't see key0, key1.
            {TopicName, Partition, #{key := <<"key0">>}}
        ],
        received_records()
    ),
    meck:reset(test_consumer_callback),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

last_with_single_message() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    % There's one message on the partition.
    kafine_kamock:produce(0, 1),

    TopicName = ?TOPIC_NAME,
    Partition = 0,
    Subscription = #{
        TopicName => {#{initial_offset => -1, offset_reset_policy => latest}, #{Partition => -1}}
    },

    kafine_consumer:subscribe(Consumer, Subscription),

    % We should see a ListOffsets, then a Fetch at zero.
    meck:wait(
        kamock_list_offsets,
        handle_list_offsets_request,
        [meck:is(has_timestamp(?LATEST_TIMESTAMP)), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(has_fetch_offset(0)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see the single message.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', '_', 1, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key0">>}}
        ],
        received_records()
    ),
    meck:reset(test_consumer_callback),

    % If we produce another message, we should see it.
    kafine_kamock:produce(0, 2),

    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', '_', 2, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key1">>}}
        ],
        received_records()
    ),
    meck:reset(test_consumer_callback),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

last_with_multiple_messages() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    % There's a bunch of messages on the partition.
    kafine_kamock:produce(10, 15),

    TopicName = ?TOPIC_NAME,
    Partition = 0,
    Subscription = #{
        TopicName => {#{initial_offset => -1, offset_reset_policy => latest}, #{Partition => -1}}
    },

    kafine_consumer:subscribe(Consumer, Subscription),

    % We should see a ListOffsets, then a Fetch.
    meck:wait(
        kamock_list_offsets,
        handle_list_offsets_request,
        [meck:is(has_timestamp(?LATEST_TIMESTAMP)), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        kamock_fetch,
        handle_fetch_request,
        [meck:is(has_fetch_offset(14)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see a single message.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', '_', 15, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key14">>}}
        ],
        received_records()
    ),
    meck:reset(test_consumer_callback),

    % If we produce another message, we should see it.
    kafine_kamock:produce(10, 16),

    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', '_', 16, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertMatch(
        [
            % We don't see key0, key1.
            {TopicName, Partition, #{key := <<"key15">>}}
        ],
        received_records()
    ),
    meck:reset(test_consumer_callback),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

has_timestamp(ExpectedTimestamp) ->
    fun(ListOffsetsRequest) ->
        #{
            topics := [
                #{partitions := [#{partition_index := 0, timestamp := Timestamp}]}
            ]
        } = ListOffsetsRequest,
        Timestamp == ExpectedTimestamp
    end.

has_fetch_offset(ExpectedOffset) ->
    fun(FetchRequest) ->
        #{
            topics := [
                #{partitions := [#{partition := 0, fetch_offset := FetchOffset}]}
            ]
        } = FetchRequest,
        FetchOffset == ExpectedOffset
    end.

received_records() ->
    lists:filtermap(
        fun
            ({_, {_, handle_record, [TopicName, Partition, Record, _]}, _}) ->
                {true, {TopicName, Partition, Record}};
            (_) ->
                false
        end,
        meck:history(test_consumer_callback)
    ).
