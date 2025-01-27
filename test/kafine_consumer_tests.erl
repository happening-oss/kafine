-module(kafine_consumer_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, ?MODULE).
-define(WAIT_TIMEOUT_MS, 2_000).

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
    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    TopicName = ?TOPIC_NAME,
    Partitions = [0, 1, 2, 3],
    Subscription = #{
        TopicName => {#{}, #{P => 0 || P <- Partitions}}
    },

    ok = kafine_consumer:subscribe(Consumer, Subscription),
    #{node_consumers := NodeConsumers} = kafine_consumer:info(Consumer),
    ?assertEqual(3, map_size(NodeConsumers)),

    % We should have three node consumers, all of which should be active.
    maps:foreach(
        fun(_NodeId, NodeConsumer) ->
            Info = kafine_node_consumer:info(NodeConsumer),
            #{state := active} = Info,
            #{topic_partitions := #{TopicName := PartitionStates}} = Info,
            maps:foreach(
                fun(_PartitionIndex, PartitionState) ->
                    #{state := active} = PartitionState
                end,
                PartitionStates
            )
        end,
        NodeConsumers
    ),

    lists:foreach(
        fun(P) ->
            meck:wait(
                test_consumer_callback, end_record_batch, ['_', P, '_', '_', '_'], ?WAIT_TIMEOUT_MS
            )
        end,
        Partitions
    ),

    kafine_consumer:stop(Consumer),
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

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    TopicName = ?TOPIC_NAME,
    Partitions = [0, 1, 2, 3],
    Subscription = #{
        TopicName => {#{}, #{P => 0 || P <- Partitions}}
    },

    ok = kafine_consumer:subscribe(Consumer, Subscription),
    #{node_consumers := NodeConsumers} = kafine_consumer:info(Consumer),
    ?assertEqual(length(NodeIds), map_size(NodeConsumers)),

    lists:foreach(
        fun(P) ->
            meck:wait(
                test_consumer_callback,
                end_record_batch,
                ['_', P, LastOffset, '_', '_'],
                ?WAIT_TIMEOUT_MS
            )
        end,
        Partitions
    ),

    kafine_consumer:stop(Consumer),
    kamock_cluster:stop(Cluster),
    ok.

subset_of_partitions() ->
    % What if we only want to subscribe to _some_ of the available partitions?
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    TopicName = ?TOPIC_NAME,
    Partitions = [0, 1],
    Subscription = #{
        TopicName => {#{}, #{P => 0 || P <- Partitions}}
    },

    ok = kafine_consumer:subscribe(Consumer, Subscription),

    % Two partitions, two calls.
    lists:foreach(
        fun(P) ->
            meck:wait(
                test_consumer_callback, end_record_batch, ['_', P, '_', '_', '_'], ?WAIT_TIMEOUT_MS
            )
        end,
        Partitions
    ),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

partition_does_not_exist() ->
    % What if we only want to subscribe to a partition that doesn't exist?
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    TopicName = ?TOPIC_NAME,
    % There are 4 partitions: 0..3; subscribe to one that exists, one that doesn't.
    Partitions = [1, 41],
    Subscription = #{
        TopicName => {#{}, #{P => 0 || P <- Partitions}}
    },

    % TODO: The partition doesn't exist. Should we expect an error here?
    ok = kafine_consumer:subscribe(Consumer, Subscription),

    % We should see two calls, both to the same partition (i.e. we skip the missing one)
    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),
    ?assertEqual(
        2, meck:num_calls(test_consumer_callback, end_record_batch, ['_', 1, '_', '_', '_'])
    ),
    ?assertEqual(
        0, meck:num_calls(test_consumer_callback, end_record_batch, ['_', 41, '_', '_', '_'])
    ),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

consumer_ref() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    ?assertEqual(
        #{node_consumers => #{}},
        kafine_consumer:info(?CONSUMER_REF)
    ),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.
