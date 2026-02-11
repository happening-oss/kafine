-module(kafine_fetcher_sup_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").

-include("assert_meck.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_ARGS, undefined).
-define(CONNECTION_OPTIONS, kafine_connection_options:validate_options(#{})).
-define(CONSUMER_OPTIONS, kafine_consumer_options:validate_options(#{})).
-define(TOPIC_OPTIONS, kafine_topic_options:validate_options([?TOPIC_NAME], #{})).
-define(FETCHER_METADATA, #{}).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    meck:new(test_fetcher_callback, [non_strict]),
    meck:expect(test_fetcher_callback, handle_partition_data, fun(_A, _T, _P, _O, _U) -> ok end),
    ok.

cleanup(_) ->
    meck:unload().

kafine_consumer_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun empty_topic/0,
        fun empty_topic_nonzero_offset/0,
        fun subset_of_partitions/0,
        fun partition_does_not_exist/0,
        fun consumer_ref/0
    ]}.

empty_topic() ->
    NodeIds = [101, 102, 103],
    {ok, Cluster, [Bootstrap | _Brokers]} = kamock_cluster:start(?CLUSTER_REF, NodeIds),

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Bootstrap, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA
    ),

    Partitions = [0, 1, 2, 3],

    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{?TOPIC_NAME => Partitions}),
    ?assertEqual(3, length(kafine_node_fetcher_sup:list_children(?CONSUMER_REF))),

    fetch_all(?CONSUMER_REF, ?TOPIC_NAME, Partitions, 0),

    lists:foreach(
        fun(P) ->
            ?assertWait(
                test_fetcher_callback, handle_partition_data, ['_', '_', for_partition(P), '_', '_'], ?WAIT_TIMEOUT_MS
            )
        end,
        Partitions
    ),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_cluster:stop(Cluster),
    ok.

empty_topic_nonzero_offset() ->
    % This test checks that we're validating offset_reset_policy properly.

    NodeIds = [101, 102, 103, 104],
    {ok, Cluster, [Bootstrap | _Brokers]} = kamock_cluster:start(?CLUSTER_REF, NodeIds),

    % There _were_ some messages on this topic, but they all got deleted.
    FirstOffset = LastOffset = 12,
    meck:expect(
        kamock_list_offsets_partition_response,
        make_list_offsets_partition_response,
        kamock_list_offsets_partition_response:range(FirstOffset, LastOffset)
    ),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        fun
            (_T, #{partition := P, fetch_offset := O}, _) when O /= LastOffset ->
                kamock_partition_data:make_error(P, ?OFFSET_OUT_OF_RANGE);
            (_T, #{partition := P, fetch_offset := _O}, _) ->
                kamock_partition_data:make_empty(P, FirstOffset, LastOffset)
        end
    ),

    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Bootstrap, ?CONNECTION_OPTIONS),
    {ok, M} = kafine_metadata_cache:start_link(?CONSUMER_REF),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA
    ),

    Partitions = [0, 1, 2, 3],


    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{?TOPIC_NAME => Partitions}),
    ?assertEqual(length(NodeIds), length(kafine_node_fetcher_sup:list_children(?CONSUMER_REF))),

    % Fetch from each partition
    lists:foreach(
        fun(Partition) ->
            kafine_fetcher:fetch(
                ?CONSUMER_REF,
                ?TOPIC_NAME,
                Partition,
                FirstOffset,
                test_fetcher_callback,
                ?CALLBACK_ARGS
            )
        end,
        Partitions
    ),

    lists:foreach(
        fun(P) ->
            ?assertWait(
                test_fetcher_callback, handle_partition_data, ['_', '_', for_partition(P), '_', '_'], ?WAIT_TIMEOUT_MS
            )
        end,
        Partitions
    ),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_cluster:stop(Cluster),
    ok.

subset_of_partitions() ->
    % What if we only want to subscribe to _some_ of the available partitions?
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

    Partitions = [0, 1],

    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{?TOPIC_NAME => Partitions}),

    fetch_all(?CONSUMER_REF, ?TOPIC_NAME, Partitions, 0),

    % Two partitions, two calls.
    lists:foreach(
        fun(P) ->
            ?assertWait(
                test_fetcher_callback, handle_partition_data, ['_', '_', for_partition(P), '_', '_'], ?WAIT_TIMEOUT_MS
            )
        end,
        Partitions
    ),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

partition_does_not_exist() ->
    % What if we only want to subscribe to a partition that doesn't exist?
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

    % There are 4 partitions: 0..3; subscribe to one that exists, one that doesn't.
    Partitions = [1, 41],

    % TODO: The partition doesn't exist. Should we expect an error here?
    ok = kafine_fetcher:set_topic_partitions(?CONSUMER_REF, #{?TOPIC_NAME => Partitions}),

    % Fetch the bad partition
    fetch_all(?CONSUMER_REF, ?TOPIC_NAME, [41], 0),

    % Now fetch the good partition and wait for a result, twice to ensure the bad one has a chance to be handled
    fetch_all(?CONSUMER_REF, ?TOPIC_NAME, [1], 0),
    ?assertWait(
        test_fetcher_callback, handle_partition_data, ['_', '_', for_partition(1), '_', '_'], ?WAIT_TIMEOUT_MS
    ),
    fetch_all(?CONSUMER_REF, ?TOPIC_NAME, [1], 0),
    ?assertWait(
        test_fetcher_callback, handle_partition_data, ['_', '_', for_partition(1), '_', '_'], ?WAIT_TIMEOUT_MS
    ),

    ?assertNotCalled(test_fetcher_callback, handle_partition_data, ['_', '_', for_partition(41), '_', '_']),

    kafine_fetcher_sup:stop(Sup),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

consumer_ref() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, B} = kafine_bootstrap:start_link(?CONSUMER_REF, Broker, ?CONNECTION_OPTIONS),
    {ok, Sup} = kafine_fetcher_sup:start_link(
        ?CONSUMER_REF,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA
    ),

    ?assertEqual(
        [],
        kafine_node_fetcher_sup:list_children(?CONSUMER_REF)
    ),

    kafine_fetcher_sup:stop(Sup),
    kafine_bootstrap:stop(B),
    kamock_broker:stop(Broker),
    ok.

fetch_all(Ref, Topic, Partitions, Offset) ->
    lists:foreach(
        fun(Partition) ->
            kafine_fetcher:fetch(
                Ref,
                Topic,
                Partition,
                Offset,
                test_fetcher_callback,
                ?CALLBACK_ARGS
            )
        end,
        Partitions
    ).

for_partition(Partition) ->
    meck:is(
        fun(#{partition_index := P}) -> P =:= Partition end
    ).
