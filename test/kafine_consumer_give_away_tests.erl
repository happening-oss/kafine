-module(kafine_consumer_give_away_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    meck:new(kamock_fetch, [passthrough]),
    meck:new(kamock_list_offsets, [passthrough]),
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

kafine_consumer_give_away_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun not_leader_or_follower/0,
        fun partitions_move_repeatedly/0,
        fun offset_reset_policy_is_preserved/0
    ]}.

not_leader_or_follower() ->
    NodeIds = [101, 102, 103],
    {ok, Cluster, Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, NodeIds),

    % When a partition moves, it's because the metadata initially told us about the wrong leader. Subsequent metadata
    % should be correct. We'll do this by having the partitions all move over by one broker.
    TopicName = ?TOPIC_NAME,
    Partitions = [0, 1, 2, 3],

    meck:expect(
        kamock_metadata,
        handle_metadata_request,
        ['_', '_'],
        meck:seq([
            fun(Req, Env) ->
                % On the first call, we'll have all of the partitions on node 101.
                NodeId = 101,
                meck:expect(
                    kamock_metadata_response_partition,
                    make_metadata_response_partition,
                    fun(PartitionIndex, _Env) ->
                        kamock_metadata_response_partition:make_metadata_response_partition(
                            PartitionIndex,
                            NodeId,
                            NodeIds
                        )
                    end
                ),
                meck:passthrough([Req, Env])
            end,
            fun(Req, Env) ->
                % On subsequent calls, we'll have all of the partitions on node 102.
                NodeId = 102,
                meck:expect(
                    kamock_metadata_response_partition,
                    make_metadata_response_partition,
                    fun(PartitionIndex, _Env) ->
                        kamock_metadata_response_partition:make_metadata_response_partition(
                            PartitionIndex,
                            NodeId,
                            NodeIds
                        )
                    end
                ),
                meck:passthrough([Req, Env])
            end
        ])
    ),

    % Any requests to node 101 fail with ?NOT_LEADER_OR_FOLLOWER; other requests succeed.
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        fun
            (_, #{partition := P}, #{node_id := 101}) ->
                kamock_partition_data:make_error(
                    P, ?NOT_LEADER_OR_FOLLOWER
                );
            (_, #{partition := P}, #{node_id := _}) ->
                kamock_partition_data:make_empty(P)
        end
    ),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        {test_consumer_callback, undefined},
        #{}
    ),

    Subscription = #{
        TopicName => {#{}, #{P => 0 || P <- Partitions}}
    },

    ok = kafine_consumer:subscribe(Consumer, Subscription),

    % We expect 2 fetch requests: a failing one and then a successful one.
    meck:wait(2, kamock_fetch, handle_fetch_request, '_', ?WAIT_TIMEOUT_MS),

    [
        % First fetch request: all partitions, node 101, return ?NOT_LEADER_OR_FOLLOWER
        {_, {_, _, FetchArgs1}, FetchRet1},

        % Second fetch request: all partitions, node 102, return ?NONE
        {_, {_, _, FetchArgs2}, FetchRet2}
    ] = meck:history(kamock_fetch),

    % Note: meck:capture(..., result) needs https://github.com/eproxus/meck/pull/250.
    %  FetchRet1 = meck:capture(1, kamock_fetch, handle_fetch_request, '_', result),
    %  FetchRet2 = meck:capture(2, kamock_fetch, handle_fetch_request, '_', result),

    assert_fetch_request_all_partitions(FetchArgs1),
    assert_fetch_request_node(101, FetchArgs1),
    assert_fetch_reply_all_partitions_error(?NOT_LEADER_OR_FOLLOWER, FetchRet1),

    assert_fetch_request_all_partitions(FetchArgs2),
    assert_fetch_request_node(102, FetchArgs2),
    assert_fetch_reply_all_partitions_error(?NONE, FetchRet2),

    % There should be 2 connections to node 101 (bootstrap and node consumer) and 1 connection to node 102 (node consumer).
    assert_active_connections(2, 101, Brokers),
    assert_active_connections(1, 102, Brokers),
    assert_active_connections(0, 103, Brokers),

    kafine_consumer:stop(Consumer),
    kamock_cluster:stop(Cluster),
    ok.

partitions_move_repeatedly() ->
    NodeIds = [101, 102, 103],
    {ok, Cluster, Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, NodeIds),

    TopicName = ?TOPIC_NAME,
    Partitions = [0, 1, 2, 3],

    % Start with the partitions on certain nodes.
    reassign_partitions(#{0 => 101, 1 => 102, 2 => 103, 3 => 101}, NodeIds),

    % Start the consumer.
    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        {test_consumer_callback, undefined},
        #{}
    ),

    Subscription = #{
        TopicName => {#{}, #{P => 0 || P <- Partitions}}
    },

    ok = kafine_consumer:subscribe(Consumer, Subscription),

    % Wait for the fetch to complete for each partition.
    Wait = fun() ->
        [
            meck:wait(
                test_consumer_callback, end_record_batch, ['_', P, '_', '_', '_'], ?WAIT_TIMEOUT_MS
            )
         || P <- Partitions
        ],
        meck:reset(test_consumer_callback)
    end,

    Wait(),

    % Move the partitions over one node.
    reassign_partitions(#{0 => 102, 1 => 103, 2 => 101, 3 => 102}, NodeIds),

    % Wait for the fetch to complete for each partition.
    Wait(),

    % Move the partitions over one node.
    reassign_partitions(#{0 => 103, 1 => 101, 2 => 102, 3 => 103}, NodeIds),

    % Wait for the fetch to complete for each partition.
    Wait(),

    % Assert that we've got a sensible number of connections.
    assert_active_connections(2, 101, Brokers),
    assert_active_connections(1, 102, Brokers),
    assert_active_connections(1, 103, Brokers),

    kafine_consumer:stop(Consumer),
    kamock_cluster:stop(Cluster),
    ok.

offset_reset_policy_is_preserved() ->
    % Test that the topic options (in this case, offset reset policy) are correctly applied after a partition moves from
    % one node consumer to another.
    %
    % Because kafine_consumer holds onto the topic options, we need to make sure they're correctly preserved if a
    % partition moves.
    %
    % We need to start a consumer with a non-default offset-reset-policy.
    % It needs to be on a single node (so that only the corresponding node consumer knows about the topic options).
    % We need to move a partition (this will result in the new node consumer getting empty options).
    % We need to trigger the offset-reset-policy.
    %
    % It's awkward to test this with records being produced, so we can just assert that the correct offset reset policy
    % got called.
    NodeIds = [101, 102, 103],
    {ok, Cluster, _Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, NodeIds),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        {test_consumer_callback, undefined},
        #{}
    ),

    TopicName = ?TOPIC_NAME,
    Subscription = #{
        TopicName => {#{offset_reset_policy => latest}, #{0 => 0}}
    },

    % Start with the partition on a particular node.
    reassign_partitions(#{0 => 101, 1 => 101, 2 => 101, 3 => 101}, NodeIds),
    ok = kafine_consumer:subscribe(Consumer, Subscription),

    meck:wait(test_consumer_callback, end_record_batch, ['_', 0, '_', '_', '_'], ?WAIT_TIMEOUT_MS),

    % Move the partitions. We've got an offset; we don't expect the reset policy to be called.
    reassign_partitions(#{0 => 102, 1 => 102, 2 => 102, 3 => 102}, NodeIds),

    % Pretend that the offset is out of range. This would usually be hard to replicate -- it involves producing records
    % and deleting old records while we're paused.
    %
    % Rather than bother reproducing all of that, we'll just return OFFSET_OUT_OF_RANGE for all offsets.
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        fun
            (_, #{partition := PartitionIndex}, #{node_id := 102}) ->
                kamock_partition_data:make_error(
                    PartitionIndex, ?OFFSET_OUT_OF_RANGE
                );
            (_, #{partition := PartitionIndex}, _Env) ->
                kamock_partition_data:make_error(
                    PartitionIndex, ?NOT_LEADER_OR_FOLLOWER
                )
        end
    ),

    % We should see a ListOffsets request.
    meck:wait(kamock_list_offsets, handle_list_offsets_request, '_', ?WAIT_TIMEOUT_MS),

    kafine_consumer:stop(Consumer),
    kamock_cluster:stop(Cluster),
    ok.

assert_fetch_request_all_partitions(Args) ->
    [
        #{
            topics := [
                #{
                    % We're expecting a fetch containing all partitions
                    partitions := [
                        #{partition := 3},
                        #{partition := 2},
                        #{partition := 1},
                        #{partition := 0}
                    ]
                }
            ]
        },
        _
    ] = Args.

assert_fetch_request_node(NodeId, Args) ->
    [
        _,
        #{node_id := NodeId}
    ] = Args.

assert_fetch_reply_all_partitions_error(ErrorCode, Ret) ->
    #{
        error_code := ?NONE,
        responses := [
            #{
                partitions := [
                    #{partition_index := 3, error_code := ErrorCode},
                    #{partition_index := 2, error_code := ErrorCode},
                    #{partition_index := 1, error_code := ErrorCode},
                    #{partition_index := 0, error_code := ErrorCode}
                ]
            }
        ]
    } = Ret.

assert_active_connections(ExpectedActiveConnections, NodeId, Brokers) ->
    Broker = get_node_by_id(Brokers, NodeId),
    assert_active_connections(ExpectedActiveConnections, Broker).

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

get_node_by_id(Nodes, NodeId) when is_list(Nodes), is_integer(NodeId) ->
    [Node] = [N || N = #{node_id := Id} <- Nodes, Id =:= NodeId],
    Node.

reassign_partitions(PartitionLeaders, NodeIds) ->
    % Note that there's an assumption that there are 4 partitions (0-3 inclusive) in a mocked topic; the
    % 'PartitionLeaders' map MUST contain all of them.
    meck:expect(
        kamock_metadata_response_partition,
        make_metadata_response_partition,
        fun(PartitionIndex, _Env) ->
            LeaderId = maps:get(PartitionIndex, PartitionLeaders),
            kamock_metadata_response_partition:make_metadata_response_partition(
                PartitionIndex, LeaderId, NodeIds
            )
        end
    ),
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        fun(_, #{partition := PartitionIndex}, #{node_id := NodeId}) ->
            case maps:get(PartitionIndex, PartitionLeaders) of
                NodeId ->
                    kamock_partition_data:make_empty(PartitionIndex);
                _ ->
                    kamock_partition_data:make_error(
                        PartitionIndex, ?NOT_LEADER_OR_FOLLOWER
                    )
            end
        end
    ),
    ok.
