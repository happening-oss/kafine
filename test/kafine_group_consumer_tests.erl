-module(kafine_group_consumer_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/timestamp.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(MEMBERSHIP_CALLBACK, kafine_group_consumer_subscription_callback).
-define(HEARTBEAT_INTERVAL_MS, 30).
-define(PROTOCOL_NAME, <<"kafine">>).
-define(PROTOCOL_TYPE, <<"kafine">>).
-define(REBALANCE_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_GENERATION_1, 1).
-define(GROUP_GENERATION_2, 2).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun leader_assign_and_fetch/0,
        fun leader_revoke_and_reassign/0,
        fun resumes_fetching_from_committed_offset/0,
        fun offset_commit_from_consumer_callback/0,
        fun membership_topic_options/0
    ]}.

setup() ->
    meck:new(test_assignment_callback, [non_strict]),
    meck:expect(test_assignment_callback, init, fun(_) -> {ok, undefined} end),
    meck:expect(test_assignment_callback, before_assignment, fun(_, _, St) -> {ok, St} end),
    meck:expect(test_assignment_callback, after_assignment, fun(_, _, St) -> {ok, St} end),

    meck:new(kamock_fetch, [passthrough]),
    meck:new(kamock_join_group, [passthrough]),
    meck:new(kamock_sync_group, [passthrough]),
    meck:new(kamock_heartbeat, [passthrough]),
    meck:new(kamock_offset_fetch, [passthrough]),
    meck:new(kamock_list_offsets, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

leader_assign_and_fetch() ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, join_group],
        [kafine, rebalance, leader]
    ]),
    Topics = [?TOPIC_NAME],
    Partitions = [0, 1, 2, 3],

    ClientId = <<"consumer_a">>,
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{client_id => ClientId},
        {kafine_consumer_callback_logger, Broker},
        #{}
    ),

    GroupId = ?GROUP_ID,
    TopicOptions = #{},
    SubscriptionCallback =
        {?MEMBERSHIP_CALLBACK, [
            Consumer, GroupId, Topics, TopicOptions, kafine_group_consumer_offset_callback
        ]},
    {ok, Rebalance} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            subscription_callback => SubscriptionCallback,
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    % Wait for the 'join_group' event.
    receive
        {[kafine, rebalance, join_group], TelemetryRef, _Measurements = #{}, Metadata} ->
            ?assertMatch(#{group_id := GroupId}, Metadata)
    end,

    % Wait until we're the leader
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    ?assertMatch({leader, _}, sys:get_state(Rebalance)),

    % Wait for the expected fetches to happen.
    meck:wait_for(
        fetched_partitions(?TOPIC_NAME, Partitions),
        kamock_fetch,
        handle_fetch_request,
        [meck:is(is_client_id(ClientId)), '_'],
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    telemetry:detach(TelemetryRef),
    kafine_eager_rebalance:stop(Rebalance),
    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

fetched_partitions(ExpectedTopic, ExpectedPartitions) ->
    Cond = fun([#{topics := Topics}, _], Expected) ->
        ActualPartitions = [
            P
         || #{topic := T, partitions := Ps} <- Topics, T =:= ExpectedTopic, #{partition := P} <- Ps
        ],
        case Expected -- ActualPartitions of
            [] -> {halt, ok};
            RemainingPartitions -> {cont, RemainingPartitions}
        end
    end,
    {Cond, ExpectedPartitions}.

is_client_id(ExpectedClientId) ->
    fun(#{client_id := ActualClientId}) ->
        ActualClientId =:= ExpectedClientId
    end.

leader_revoke_and_reassign() ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, leader]
    ]),
    Topic = ?TOPIC_NAME,
    Topics = [Topic],

    ClientId = <<"consumer_a">>,
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_leader(?GROUP_GENERATION_1)
    ),

    % Assign [0,1] and [2,3]
    Partitions01 = [0, 1],
    Partitions23 = [2, 3],
    Assignment01 = kafcod_consumer_protocol:encode_assignment(
        [#{topic => ?TOPIC_NAME, partitions => Partitions01}], <<>>
    ),
    Assignment23 = kafcod_consumer_protocol:encode_assignment(
        [#{topic => ?TOPIC_NAME, partitions => Partitions23}], <<>>
    ),

    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        ['_', '_'],
        fun(_Req = #{correlation_id := CorrelationId}, _Env) ->
            #{
                protocol_name => ?PROTOCOL_NAME,
                protocol_type => ?PROTOCOL_TYPE,
                correlation_id => CorrelationId,
                error_code => 0,
                throttle_time_ms => 0,

                assignment => Assignment01
            }
        end
    ),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{client_id => ClientId},
        {kafine_consumer_callback_logger, Broker},
        #{}
    ),

    GroupId = ?GROUP_ID,
    TopicOptions = #{},
    SubscriptionCallback =
        {?MEMBERSHIP_CALLBACK, [
            Consumer, GroupId, Topics, TopicOptions, kafine_group_consumer_offset_callback
        ]},
    {ok, Rebalance} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback => SubscriptionCallback,
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    % Wait until we're the leader
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % Make sure we have a node consumer
    #{node_consumers := #{101 := NodeConsumer}} = kafine_consumer:info(Consumer),
    % Make sure the node consumers are consuming from the right topic partitions
    #{topic_partitions := #{Topic := #{0 := _, 1 := _}}} = kafine_node_consumer:info(NodeConsumer),

    % Wait for the expected fetches to happen
    meck:wait_for(
        fetched_partitions(?TOPIC_NAME, Partitions01),
        kamock_fetch,
        handle_fetch_request,
        [meck:is(is_client_id(ClientId)), '_'],
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    % Set up the new assignments.
    meck:expect(
        kamock_sync_group,
        handle_sync_group_request,
        ['_', '_'],
        fun(_Req = #{correlation_id := CorrelationId}, _Env) ->
            #{
                protocol_name => ?PROTOCOL_NAME,
                protocol_type => ?PROTOCOL_TYPE,
                correlation_id => CorrelationId,
                error_code => 0,
                throttle_time_ms => 0,

                assignment => Assignment23
            }
        end
    ),

    meck:expect(
        kamock_join_group,
        handle_join_group_request,
        kamock_join_group:as_leader(?GROUP_GENERATION_2)
    ),

    % Trigger a rebalance.
    meck:expect(
        kamock_heartbeat,
        handle_heartbeat_request,
        kamock_heartbeat:expect_generation_id(?GROUP_GENERATION_2)
    ),

    % Wait until we're the leader
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % Make sure the node consumers are consuming from the right topic partitions
    #{node_consumers := #{101 := NodeConsumer2}} = kafine_consumer:info(Consumer),
    #{topic_partitions := #{Topic := #{2 := _, 3 := _}}} = kafine_node_consumer:info(NodeConsumer2),

    % Wait for the new expected fetches to happen; twice. which is why this tests fails, because the second bout is
    % non-deterministic.
    meck:wait_for(
        fetched_partitions(?TOPIC_NAME, Partitions23),
        kamock_fetch,
        handle_fetch_request,
        [meck:is(is_client_id(ClientId)), '_'],
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    telemetry:detach(TelemetryRef),
    kafine_eager_rebalance:stop(Rebalance),
    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

resumes_fetching_from_committed_offset() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    Topics = [?TOPIC_NAME],
    Partitions = [0, 1, 2, 3],
    % Pretend like we committed offsets to each partition
    meck:expect(
        kamock_offset_fetch,
        handle_offset_fetch_request,
        fun(#{correlation_id := CorrelationId}, _) ->
            #{
                correlation_id => CorrelationId,
                throttle_time_ms => 0,
                error_code => ?NONE,
                topics => [
                    #{
                        name => Topic,
                        partitions => [
                            #{
                                partition_index => P,
                                error_code => ?NONE,
                                committed_offset => CO,
                                metadata => <<"">>
                            }
                         || P = CO <- Partitions
                        ]
                    }
                 || Topic <- Topics
                ]
            }
        end
    ),
    % Start the group consumer
    ClientId = <<"consumer_a">>,
    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{client_id => ClientId},
        {kafine_consumer_callback_logger, []},
        #{}
    ),

    GroupId = ?GROUP_ID,
    TopicOptions = #{},
    SubscriptionCallback =
        {?MEMBERSHIP_CALLBACK, [
            Consumer, GroupId, Topics, TopicOptions, kafine_group_consumer_offset_callback
        ]},
    {ok, Rebalance} = kafine_eager_rebalance:start_link(
        ?REBALANCE_REF,
        Broker,
        #{},
        GroupId,
        #{
            subscription_callback => SubscriptionCallback,
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    % Check if we started fetching from the committed offsets
    meck:wait(
        1,
        kamock_fetch,
        handle_fetch_request,
        [
            meck:is(expected_fetch_request_fetch_offsets(?TOPIC_NAME)),
            '_'
        ],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_eager_rebalance:stop(Rebalance),
    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),

    ok.

expected_fetch_request_fetch_offsets(Topic) ->
    fun(
        #{
            client_id := <<"consumer_a">>,
            topics :=
                [
                    #{
                        topic := T,
                        partitions := Ps
                    }
                ]
        }
    ) when T =:= Topic ->
        lists:all(fun(#{partition := P, fetch_offset := FO}) -> P =:= FO end, Ps)
    end.

offset_commit_from_consumer_callback() ->
    % Setup committing offsets
    ets:new(committed_offsets, [named_table, public]),
    meck:expect(
        kamock_offset_commit,
        handle_offset_commit_request,
        kamock_offset_commit:to_ets(committed_offsets)
    ),
    meck:expect(
        kamock_offset_fetch,
        handle_offset_fetch_request,
        kamock_offset_fetch:from_ets(committed_offsets)
    ),
    % Setup consumer callback
    meck:new(offset_commit_callback, [non_strict]),
    meck:expect(offset_commit_callback, init, fun(_T, _P, [Ref]) ->
        {ok, {Ref, []}}
    end),
    meck:expect(offset_commit_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    % We need to batch messages ourselves
    meck:expect(offset_commit_callback, handle_record, fun(_T, _P, M, {Ref, Ms}) ->
        {ok, {Ref, [Ms] ++ [M]}}
    end),
    % We commit offsets in the consumer callback end_record_batch
    Topics = [?TOPIC_NAME],
    meck:expect(
        offset_commit_callback,
        end_record_batch,
        fun(T, P, _N, _Info, St = {Ref, Ms}) ->
            % Commit the offset of the last message in batch. This is what kafire does; it's unconventional.
            #{offset := O} = lists:last(Ms),
            Offsets = #{T => #{P => O}},
            kafine_eager_rebalance:offset_commit(Ref, Offsets),
            {ok, St}
        end
    ),
    % 'Produce' up to offset 2 (offset 2 is an empty message)
    MessageBuilder = fun(T, Partition, Offset) ->
        MessageId = iolist_to_binary(
            io_lib:format("~s-~B-~B", [T, Partition, Offset])
        ),
        #{key => MessageId, value => MessageId}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:range(0, 2, MessageBuilder)
    ),
    % Start consumer and membership
    ClientId = <<"consumer_a">>,
    GroupId = ?GROUP_ID,
    Ref = ?REBALANCE_REF,
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{client_id => ClientId},
        {offset_commit_callback, [Ref]},
        #{}
    ),

    TopicOptions = #{},
    SubscriptionCallback =
        {?MEMBERSHIP_CALLBACK, [
            Consumer, GroupId, Topics, TopicOptions, kafine_group_consumer_offset_callback
        ]},
    {ok, Rebalance} = kafine_eager_rebalance:start_link(
        Ref,
        Broker,
        #{},
        GroupId,
        #{
            subscription_callback => SubscriptionCallback,
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),
    % Wait for the messages to be processed
    % by default, end_record_batch is called once per partition per message on the mock broker
    % so waiting for it be called 8 times means it will process 2 messages on each partition
    meck:wait(8, offset_commit_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    % Make a offset fetch call to assert that we committed the offsets
    {ok, C} = kafine_connection:start_link(Broker, #{}),
    {ok, #{error_code := ?NONE, topics := TopicsResponse}} = kafine_connection:call(
        C,
        fun offset_fetch_request:encode_offset_fetch_request_4/1,
        #{
            group_id => GroupId,
            topics => [#{name => ?TOPIC_NAME, partition_indexes => [0, 1, 2, 3]}]
        },
        fun offset_fetch_response:decode_offset_fetch_response_4/1
    ),
    kafine_connection:stop(C),
    % committed_offset is 1 because we wait for end_record_batch to be called twice for each partition (2 processsed messages)
    ExpectedResponse = [
        #{
            name => ?TOPIC_NAME,
            partitions =>
                [
                    #{
                        metadata => <<>>,
                        error_code => ?NONE,
                        partition_index => P,
                        committed_offset => 1
                    }
                 || P <- [0, 1, 2, 3]
                ]
        }
    ],

    ?assertEqual(ExpectedResponse, TopicsResponse),
    % Cleanup
    kafine_eager_rebalance:stop(Rebalance),
    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

membership_topic_options() ->
    % This test wants to assert that topic options get passed down to the subscription successfully
    % We don't really care about whats in the topic options
    % The kafine consumer tests test the actual functionality of topic options

    % Start consumer and membership
    ClientId = <<"consumer_a">>,
    GroupId = ?GROUP_ID,
    Ref = ?REBALANCE_REF,
    % Two topics to test different ORPs
    Topic = <<"latest-topic">>,
    Topic2 = <<"earliest-topic">>,
    Topics = [Topic, Topic2],
    TopicOptions = #{
        Topic => #{offset_reset_policy => latest},
        Topic2 => #{offset_reset_policy => earliest}
    },

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{client_id => ClientId},
        {kafine_consumer_callback_logger, []},
        #{}
    ),
    SubscriptionCallback =
        {?MEMBERSHIP_CALLBACK, [
            Consumer, GroupId, Topics, TopicOptions, kafine_group_consumer_offset_callback
        ]},
    {ok, Rebalance} = kafine_eager_rebalance:start_link(
        Ref,
        Broker,
        #{},
        GroupId,
        #{
            subscription_callback => SubscriptionCallback,
            assignment_callback => {test_assignment_callback, undefined}
        },
        [Topic, Topic2]
    ),

    meck:wait(
        kamock_list_offsets,
        handle_list_offsets_request,
        [meck:is(expected_list_offset_request()), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Cleanup
    kafine_eager_rebalance:stop(Rebalance),
    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

expected_list_offset_request() ->
    fun(
        #{
            topics :=
                [
                    #{
                        name := <<"latest-topic">>,
                        partitions := LatestPs
                    },
                    #{
                        name := <<"earliest-topic">>,
                        partitions := EarliestPs
                    }
                ]
        }
    ) ->
        lists:all(fun(#{timestamp := Timestamp}) -> Timestamp =:= ?LATEST_TIMESTAMP end, LatestPs),
        lists:all(
            fun(#{timestamp := Timestamp}) -> Timestamp =:= ?EARLIEST_TIMESTAMP end, EarliestPs
        )
    end.
