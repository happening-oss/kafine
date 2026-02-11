-module(kafine_fetcher_set_topic_partitions_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_2_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CONNECTION_OPTIONS, kafine_connection_options:validate_options(#{})).
-define(CONSUMER_OPTIONS, kafine_consumer_options:validate_options(#{})).
-define(TOPIC_OPTIONS, kafine_topic_options:validate_options([?TOPIC_NAME, ?TOPIC_NAME_2], #{})).
-define(FETCHER_METADATA, #{}).
-define(CALLBACK_ARGS, undefined).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun subscribe_twice/0,
        fun subscribe_multiple/0,
        fun unsubscribe/0,
        fun unsubscribe_partial/0
    ]}.

setup() ->
    meck:new(test_fetcher_callback, [non_strict]),
    meck:expect(test_fetcher_callback, handle_partition_data, fun(_A, _T, _P, _O, _U) -> ok end),
    ok.

cleanup(_) ->
    meck:unload().

% TODO: Some tests around subscribing to things you're _already_ subscribed to...

subscribe_twice() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA
    ),

    % All of the partitions are on a single node. Subscribe to two different topics; we should reuse node consumers.
    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{?TOPIC_NAME => [0]}),
    fetch(?CONSUMER_REF, ?TOPIC_NAME, 0, 0),
    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{?TOPIC_NAME => [0], ?TOPIC_NAME_2 => [0]}),
    fetch(?CONSUMER_REF, ?TOPIC_NAME_2, 0, 0),

    meck:wait(2, test_fetcher_callback, handle_partition_data, '_', ?WAIT_TIMEOUT_MS),

    % 2 connections - one for the bootstrap, one for the single node fetcher
    assert_active_connections(2, Broker),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

subscribe_multiple() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA
    ),

    % All of the partitions are on a single node. Subscribe to two different topics; we should reuse node consumers.
    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{?TOPIC_NAME => [0], ?TOPIC_NAME_2 => [0]}),
    fetch(?CONSUMER_REF, ?TOPIC_NAME, 0, 0),
    fetch(?CONSUMER_REF, ?TOPIC_NAME_2, 0, 0),

    meck:wait(2, test_fetcher_callback, handle_partition_data, '_', ?WAIT_TIMEOUT_MS),

    assert_active_connections(2, Broker),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

unsubscribe() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA
    ),

    % All of the partitions are on a single node. Subscribe to two different topics; we should reuse node consumers.
    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{?TOPIC_NAME => [0], ?TOPIC_NAME_2 => [0]}),
    fetch(?CONSUMER_REF, ?TOPIC_NAME, 0, 0),
    fetch(?CONSUMER_REF, ?TOPIC_NAME_2, 0, 0),

    meck:wait(2, test_fetcher_callback, handle_partition_data, '_', ?WAIT_TIMEOUT_MS),

    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{}),

    % node consumers should have been stopped
    ?assertEqual([], kafine_node_fetcher_sup:list_children(?CONSUMER_REF)),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

unsubscribe_partial() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA
    ),

    % All of the partitions are on a single node. Subscribe to two different topics; we should reuse node consumers.
    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{?TOPIC_NAME => [0], ?TOPIC_NAME_2 => [0]}),
    fetch(?CONSUMER_REF, ?TOPIC_NAME, 0, 0),
    fetch(?CONSUMER_REF, ?TOPIC_NAME_2, 0, 0),

    meck:wait(2, test_fetcher_callback, handle_partition_data, '_', ?WAIT_TIMEOUT_MS),

    meck:reset(test_fetcher_callback),
    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{?TOPIC_NAME => [0]}),
    fetch(?CONSUMER_REF, ?TOPIC_NAME, 0, 0),

    meck:wait(test_fetcher_callback, handle_partition_data, '_', ?WAIT_TIMEOUT_MS),

    % 2 connections - one for the bootstrap, one for the single node fetcher
    assert_active_connections(2, Broker),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
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

fetch(Ref, Topic, Partition, Offset) ->
    kafine_fetcher:fetch(
        Ref,
        Topic,
        Partition,
        Offset,
        test_fetcher_callback,
        ?CALLBACK_ARGS
    ).
