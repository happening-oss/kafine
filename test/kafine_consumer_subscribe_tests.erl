-module(kafine_consumer_subscribe_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(NODE_ID, 101).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_2_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_STATE, ?MODULE).
-define(CALLBACK_ARGS, undefined).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun subscribe_twice/0,
        fun subscribe_multiple/0,
        fun unsubscribe/0,
        fun unsubscribe_all/0
    ]}.

setup() ->
    meck:new(kamock_fetch, [passthrough]),
    meck:new(kamock_partition_data, [passthrough]),
    meck:new(kamock_metadata_response_partition, [passthrough]),

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

% TODO: Some tests around subscribing to things you're _already_ subscribed to...

subscribe_twice() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % All of the partitions are on a single node. Subscribe to two different topics; we should reuse node consumers.
    Topic1 = ?TOPIC_NAME,
    Topic2 = ?TOPIC_NAME_2,

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),
    ok = kafine_consumer:subscribe(Consumer, #{Topic1 => {#{}, #{0 => 0}}}),
    ok = kafine_consumer:subscribe(Consumer, #{Topic2 => {#{}, #{0 => 0}}}),

    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    assert_active_connections(2, Broker),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

subscribe_multiple() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % All of the partitions are on a single node. Subscribe to two different topics; we should reuse node consumers.
    Topic1 = ?TOPIC_NAME,
    Topic2 = ?TOPIC_NAME_2,

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),
    ok = kafine_consumer:subscribe(Consumer, #{
        Topic1 => {#{}, #{0 => 0}},
        Topic2 => {#{}, #{0 => 0}}
    }),

    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    assert_active_connections(2, Broker),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

unsubscribe() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % All of the partitions are on a single node. Subscribe to two different topics; we should reuse node consumers.
    Topic1 = ?TOPIC_NAME,

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),
    ok = kafine_consumer:subscribe(Consumer, #{
        Topic1 => {#{}, #{0 => 0}}
    }),

    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    Unsubscription = #{Topic1 => [0]},
    ok = kafine_consumer:unsubscribe(Consumer, Unsubscription),

    % To verify that we're not fetching, we should check that we have no active node consumers. See
    % kafine_node_consumer_subscribe_tests for why we sleep for 75ms.
    timer:sleep(75),
    #{node_consumers := #{?NODE_ID := NodeConsumer}} = kafine_consumer:info(Consumer),
    ?assertEqual(
        #{state => idle, topic_partitions => #{}}, kafine_node_consumer:info(NodeConsumer)
    ),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

unsubscribe_all() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % All of the partitions are on a single node. Subscribe to two different topics; we should reuse node consumers.
    Topic1 = ?TOPIC_NAME,
    Topic2 = ?TOPIC_NAME_2,

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),
    ok = kafine_consumer:subscribe(Consumer, #{
        Topic1 => {#{}, #{0 => 0}},
        Topic2 => {#{}, #{0 => 0}}
    }),

    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    ok = kafine_consumer:unsubscribe_all(Consumer),

    % To verify that we're not fetching, we should check that we have no active node consumers. See
    % kafine_node_consumer_subscribe_tests for why we sleep for 75ms.
    timer:sleep(75),
    #{node_consumers := #{?NODE_ID := NodeConsumer}} = kafine_consumer:info(Consumer),
    ?assertEqual(
        #{state => idle, topic_partitions => #{}}, kafine_node_consumer:info(NodeConsumer)
    ),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

assert_active_connections(ExpectedActiveConnections, Broker) ->
    #{ref := Ref, node_id := NodeId} = Broker,
    #{active_connections := ActiveConnections} = kamock_broker:info(Ref),
    % Not displayed by unite_compact, unfortunately.
    Comment = #{
        node_id => NodeId,
        expected_connections => ExpectedActiveConnections,
        active_connections => ActiveConnections
    },
    ?assertEqual(ExpectedActiveConnections, ActiveConnections, Comment),
    ok.
