-module(kafine_producer_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include("kafine_eqwalizer.hrl").
-include("assert_meck.hrl").
-include("assert_received.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(PRODUCER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_t_2", [?MODULE, ?FUNCTION_NAME]))).
-define(CONNECTION_OPTIONS, #{}).
-define(PRODUCER_OPTIONS, #{linger_ms => 0}).
-define(PARTITION_1, 1).
-define(PARTITION_2, 2).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    kafine_producer_sup_sup:start_link(),
    meck:new(kamock_partition_produce_response, [passthrough]),
    meck:new(kamock_metadata, [passthrough]),
    meck:new(kamock_produce, [passthrough]),
    meck:new(kafine_backoff, [passthrough]),
    ok.

cleanup(_) ->
    kafine_producer_sup_sup:stop(),
    meck:unload().

kafine_node_producer_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun produce_sync/0,
        fun produce_async_check_response/0,
        fun produce_async_wait_response/0,
        fun produce_async_many_wait_all_responses/0,
        fun produce_async_requests_are_batched/0,
        fun will_refresh_metadata_on_not_leader_error/0,
        fun only_one_metadata_refresh_when_multiple_leaders_return_not_leader_error/0,
        fun will_retry_on_error/0,
        fun will_not_reorder_messages_on_retry/0,
        fun will_retry_up_to_max/0,
        fun produces_batch_due_to_batch_size/0,
        fun produces_multiple_batches_due_to_max_size/0,
        fun creates_telemetry/0,
        fun info/0
    ]}.

produce_sync() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    ?assertEqual(
        ok,
        kafine_producer:produce_sync(?PRODUCER_REF, ?TOPIC_NAME, ?PARTITION_1, #{
            key => <<"key">>,
            value => <<"value">>
        })
    ),

    ?assertCalled(kamock_produce, handle_produce_request, [
        #{
            topic_data => [
                #{
                    name => ?TOPIC_NAME,
                    partition_data => [
                        #{
                            index => ?PARTITION_1,
                            records => [
                                #{
                                    records => [
                                        #{
                                            key => <<"key">>,
                                            value => <<"value">>
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        '_'
    ]),

    kafine:stop_producer(?PRODUCER_REF),
    ok.

produce_async_check_response() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    ReqIds = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key">>,
            value => <<"value">>
        },
        test_label,
        kafine_producer:reqids_new()
    ),

    receive
        Msg ->
            ?assertEqual(
                {{reply, ok}, test_label, kafine_producer:reqids_new()},
                kafine_producer:check_response(Msg, ReqIds)
            )
    after ?WAIT_TIMEOUT_MS ->
        error(timeout)
    end,

    ?assertCalled(kamock_produce, handle_produce_request, [
        #{
            topic_data => [
                #{
                    name => ?TOPIC_NAME,
                    partition_data => [
                        #{
                            index => ?PARTITION_1,
                            records => [
                                #{
                                    records => [
                                        #{
                                            key => <<"key">>,
                                            value => <<"value">>
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        '_'
    ]),

    kafine:stop_producer(?PRODUCER_REF),
    ok.

produce_async_wait_response() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    ReqIds = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key">>,
            value => <<"value">>
        },
        test_label,
        kafine_producer:reqids_new()
    ),

    ?assertEqual(
        {{reply, ok}, test_label, kafine_producer:reqids_new()},
        kafine_producer:wait_response(ReqIds, ?WAIT_TIMEOUT_MS)
    ),

    kafine:stop_producer(?PRODUCER_REF),
    ok.

produce_async_many_wait_all_responses() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    ReqIds1 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key1">>,
            value => <<"value1">>
        },
        test_label_1,
        kafine_producer:reqids_new()
    ),

    ReqIds2 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_2,
        #{
            key => <<"key2">>,
            value => <<"value2">>
        },
        test_label_2,
        ReqIds1
    ),

    ReqIds3 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME_2,
        ?PARTITION_1,
        #{
            key => <<"key3">>,
            value => <<"value3">>
        },
        test_label_3,
        ReqIds2
    ),

    ReqIds4 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key4">>,
            value => <<"value4">>
        },
        test_label_4,
        ReqIds3
    ),

    ?assertEqual(
        {
            ok,
            #{
                ?TOPIC_NAME => #{
                    ?PARTITION_1 => [{test_label_1, ok}, {test_label_4, ok}],
                    ?PARTITION_2 => [{test_label_2, ok}]
                },
                ?TOPIC_NAME_2 => #{
                    ?PARTITION_1 => [{test_label_3, ok}]
                }
            },
            kafine_producer:reqids_new()
        },
        kafine_producer:wait_all_responses(ReqIds4, ?WAIT_TIMEOUT_MS)
    ),

    kafine:stop_producer(?PRODUCER_REF),
    ok.

produce_async_requests_are_batched() ->
    {ok, _, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    ProducerOptions = #{linger_ms => 5},
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Bootstrap, ?CONNECTION_OPTIONS, ProducerOptions),

    ReqIds1 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key1">>,
            value => <<"value1">>
        },
        test_label_1,
        kafine_producer:reqids_new()
    ),

    ReqIds2 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_2,
        #{
            key => <<"key2">>,
            value => <<"value2">>
        },
        test_label_2,
        ReqIds1
    ),

    ReqIds3 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME_2,
        ?PARTITION_1,
        #{
            key => <<"key3">>,
            value => <<"value3">>
        },
        test_label_3,
        ReqIds2
    ),

    ReqIds4 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key4">>,
            value => <<"value4">>
        },
        test_label_4,
        ReqIds3
    ),

    ?assertMatch({ok, _, _}, kafine_producer:wait_all_responses(ReqIds4, ?WAIT_TIMEOUT_MS)),

    % All the partition 1 requests should have arrived in one batch
    ?assertCalled(kamock_produce, handle_produce_request, [
        #{
            topic_data => [
                #{
                    name => ?TOPIC_NAME,
                    partition_data => [
                        #{
                            index => ?PARTITION_1,
                            records => [
                                #{
                                    records => [
                                        #{
                                            key => <<"key1">>,
                                            value => <<"value1">>
                                        },
                                        #{
                                            key => <<"key4">>,
                                            value => <<"value4">>
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },
                #{
                    name => ?TOPIC_NAME_2,
                    partition_data => [
                        #{
                            index => ?PARTITION_1,
                            records => [
                                #{
                                    records => [
                                        #{
                                            key => <<"key3">>,
                                            value => <<"value3">>
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        #{node_id => 102}
    ]),

    % The partition 2 request should have arrived at the other broker
    ?assertCalled(kamock_produce, handle_produce_request, [
        #{
            topic_data => [
                #{
                    name => ?TOPIC_NAME,
                    partition_data => [
                        #{
                            index => ?PARTITION_2,
                            records => [
                                #{
                                    records => [
                                        #{
                                            key => <<"key2">>,
                                            value => <<"value2">>
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        #{node_id => 101}
    ]),

    kafine:stop_producer(?PRODUCER_REF),
    ok.

will_refresh_metadata_on_not_leader_error() ->
    NodeIds = [101, 102],
    {ok, _, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, NodeIds),

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
        kamock_partition_produce_response,
        make_partition_produce_response,
        fun
            (_, #{index := P}, #{node_id := 101}) ->
                kamock_partition_produce_response:make_error_response(
                    P, ?NOT_LEADER_OR_FOLLOWER
                );
            (_, #{index := P}, #{node_id := _}) ->
                kamock_partition_produce_response:make_error_response(
                    P, ?NONE
                )
        end
    ),

    {ok, _} = kafine:start_producer(
        ?PRODUCER_REF, Bootstrap, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS
    ),

    % eventually produce succeeds
    ok = kafine_producer:produce_sync(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key">>,
            value => <<"value">>
        }
    ),

    % metadata should've been fetched twice
    MetadataHistory = meck:history(kamock_metadata),
    ?assertMatch(2, length(MetadataHistory)),

    % should only be 2 produce requests
    ProduceHistory = meck:history(kamock_produce),
    ?assertMatch(2, length(ProduceHistory)),

    ExpectedProduceRequest = #{
        topic_data => [
            #{
                name => ?TOPIC_NAME,
                partition_data => [
                    #{
                        index => ?PARTITION_1,
                        records => [
                            #{
                                records => [
                                    #{
                                        key => <<"key">>,
                                        value => <<"value">>
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    },

    % node 101 should have received the produce
    ?assertCalled(kamock_produce, handle_produce_request, [
        ExpectedProduceRequest,
        #{node_id => 101}
    ]),

    % and so should node 2
    ?assertCalled(kamock_produce, handle_produce_request, [
        ExpectedProduceRequest,
        #{node_id => 102}
    ]),

    % Specifically for NOT_LEADER_OR_FOLLOWER, we bypass the backoff because we think we understand
    % how to resolve this (send to a different leader)
    ?assertNotCalled(kafine_backoff, backoff, '_'),

    ok.

only_one_metadata_refresh_when_multiple_leaders_return_not_leader_error() ->
    NodeIds = [101, 102],
    {ok, _, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, NodeIds),

    meck:expect(
        kamock_metadata,
        handle_metadata_request,
        ['_', '_'],
        meck:seq([
            fun(Req, Env) ->
                % On the first call, even partitions go to 101, odd to 102
                meck:expect(
                    kamock_metadata_response_partition,
                    make_metadata_response_partition,
                    fun(PartitionIndex, _Env) ->
                        NodeId =
                            case PartitionIndex rem 2 of
                                0 -> 101;
                                1 -> 102
                            end,
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
                % On subsequent calls, odd partitions goe to 101, even to 102
                meck:expect(
                    kamock_metadata_response_partition,
                    make_metadata_response_partition,
                    fun(PartitionIndex, _Env) ->
                        NodeId =
                            case PartitionIndex rem 2 of
                                0 -> 102;
                                1 -> 101
                            end,
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

    % Odd requests to 102 and even to 101 fail with ?NOT_LEADER_OR_FOLLOWER; other requests succeed.
    meck:expect(
        kamock_partition_produce_response,
        make_partition_produce_response,
        fun(_, #{index := P}, #{node_id := NodeId}) ->
            % we need these to take a little while so that the second produce gets pushed before the
            % first response arrives
            timer:sleep(50),
            case {P rem 2, NodeId} of
                {0, 101} ->
                    kamock_partition_produce_response:make_error_response(
                        P, ?NOT_LEADER_OR_FOLLOWER
                    );
                {1, 102} ->
                    kamock_partition_produce_response:make_error_response(
                        P, ?NOT_LEADER_OR_FOLLOWER
                    );
                _ ->
                    kamock_partition_produce_response:make_error_response(
                        P, ?NONE
                    )
            end
        end
    ),

    {ok, _} = kafine:start_producer(
        ?PRODUCER_REF, Bootstrap, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS
    ),

    ReqIds0 = kafine_producer:reqids_new(),

    % produce to partitions 1 and 2, these will go to different nodes and both will return NOT_LEADER_OR_FOLLOWER
    ReqIds1 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key1">>,
            value => <<"value1">>
        },
        undefined,
        ReqIds0
    ),

    ReqIds2 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_2,
        #{
            key => <<"key2">>,
            value => <<"value2">>
        },
        undefined,
        ReqIds1
    ),

    % Both should eventually succeed
    ?assertMatch({ok, _, _}, kafine_producer:wait_all_responses(ReqIds2, ?WAIT_TIMEOUT_MS)),

    % metadata should only have been fetched twice
    MetadataHistory = meck:history(kamock_metadata),
    ?assertMatch(2, length(MetadataHistory)),

    % should only be 4 produce requests
    ProduceHistory = meck:history(kamock_produce),
    ?assertMatch(4, length(ProduceHistory)),

    ok.

will_retry_on_error() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    RetryBackoff = kafine_backoff:exponential(#{initial_ms => 1, max_ms => infinity}),
    ProducerOptions = kafine_producer_options:validate_options(#{retry_backoff => RetryBackoff}),
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ProducerOptions),

    meck:expect(
        kamock_partition_produce_response,
        make_partition_produce_response,
        ['_', '_', '_'],
        meck:seq([
            kamock_partition_produce_response:return_error(?CORRUPT_MESSAGE),
            kamock_partition_produce_response:return_error(?LEADER_NOT_AVAILABLE),
            kamock_partition_produce_response:return_error(?REQUEST_TIMED_OUT),
            kamock_partition_produce_response:return_error(?NOT_ENOUGH_REPLICAS),
            kamock_partition_produce_response:return_error(?NOT_ENOUGH_REPLICAS_AFTER_APPEND),
            kamock_partition_produce_response:return_error(?KAFKA_STORAGE_ERROR),
            kamock_partition_produce_response:return_error(?THROTTLING_QUOTA_EXCEEDED),
            % This error isn't retryable
            kamock_partition_produce_response:return_error(?UNKNOWN_SERVER_ERROR)
        ])
    ),

    % eventually produce fails with the non-retryable error
    {error, {kafka_error, ?UNKNOWN_SERVER_ERROR}} = kafine_producer:produce_sync(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key">>,
            value => <<"value">>
        }
    ),

    % ensure we've been invoking backoff correctly
    BackoffHistory0 = meck:history(kafine_backoff),
    BackoffHistory = lists:filter(
        fun
            ({_, {kafine_backoff, Fun, _}, _}) when Fun == init; Fun == backoff; Fun == reset ->
                true;
            (_) ->
                false
        end,
        BackoffHistory0
    ),

    ?assertMatch(
        [
            % Ignore 2 spurious backoff calls from kafine_bootstrap
            _,
            _,
            {_, {kafine_backoff, init, [RetryBackoff]}, State1},
            {_, {kafine_backoff, backoff, [State1]}, {1, State2}},
            {_, {kafine_backoff, backoff, [State2]}, {2, State3}},
            {_, {kafine_backoff, backoff, [State3]}, {4, State4}},
            {_, {kafine_backoff, backoff, [State4]}, {8, State5}},
            {_, {kafine_backoff, backoff, [State5]}, {16, State6}},
            {_, {kafine_backoff, backoff, [State6]}, {32, State7}},
            {_, {kafine_backoff, backoff, [State7]}, {64, _State8}}
        ],
        BackoffHistory
    ),

    % Should have been produced 8 times
    History = meck:history(kamock_produce),
    ?assertEqual(8, length(History)),
    ok.

will_not_reorder_messages_on_retry() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    % First message takes a while, then returns a retryable error
    % All messages after that succeed
    meck:expect(
        kamock_partition_produce_response,
        make_partition_produce_response,
        ['_', '_', '_'],
        meck:seq([
            fun(T, PD, E) ->
                timer:sleep(50),
                Fun = kamock_partition_produce_response:return_error(?REQUEST_TIMED_OUT),
                Fun(T, PD, E)
            end,
            meck:passthrough()
        ])
    ),

    % Async produce 2 messages in quick succession. linger_ms = 0 so the first will be send
    % immediately, and the second should be with the producer before the first (slow) response
    % arrives
    ReqIds0 = kafine_producer:reqids_new(),
    ReqIds1 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key">>,
            value => <<"value1">>
        },
        test_label_1,
        ReqIds0
    ),

    ReqIds2 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key">>,
            value => <<"value2">>
        },
        test_label_2,
        ReqIds1
    ),

    % Both produces should be successful
    ?assertEqual(
        {
            ok,
            #{
                ?TOPIC_NAME => #{
                    ?PARTITION_1 => [
                        {test_label_1, ok},
                        {test_label_2, ok}
                    ]
                }
            },
            kafine_producer:reqids_new()
        },
        kafine_producer:wait_all_responses(ReqIds2, ?WAIT_TIMEOUT_MS)
    ),

    % Should have two produces for the first (retried) message, followed by one for the second
    ?assertMatch(
        [
            {_,
                {_, _, [
                    #{
                        topic_data := [
                            #{
                                partition_data := [
                                    #{records := [#{records := [#{value := <<"value1">>}]}]}
                                ]
                            }
                        ]
                    },
                    _
                ]},
                _},
            {_,
                {_, _, [
                    #{
                        topic_data := [
                            #{
                                partition_data := [
                                    #{records := [#{records := [#{value := <<"value1">>}]}]}
                                ]
                            }
                        ]
                    },
                    _
                ]},
                _},
            {_,
                {_, _, [
                    #{
                        topic_data := [
                            #{
                                partition_data := [
                                    #{records := [#{records := [#{value := <<"value2">>}]}]}
                                ]
                            }
                        ]
                    },
                    _
                ]},
                _}
        ],
        meck:history(kamock_produce)
    ),

    ok.

will_retry_up_to_max() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    MaxCount = 3,
    RetryBackoff = kafine_backoff:exponential(#{initial_ms => 1, max_count => MaxCount}),
    ProducerOptions = kafine_producer_options:validate_options(#{retry_backoff => RetryBackoff}),
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ProducerOptions),

    meck:expect(
        kamock_partition_produce_response,
        make_partition_produce_response,
        kamock_partition_produce_response:return_error(?KAFKA_STORAGE_ERROR)
    ),

    % produce will fail after one attempt
    {error, {kafka_error, ?KAFKA_STORAGE_ERROR}} = kafine_producer:produce_sync(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key">>,
            value => <<"value">>
        }
    ),

    History = meck:history(kamock_produce),
    % Initial Request + MaxRetries
    ?assertEqual(MaxCount + 1, length(History)),

    ok.

produces_batch_due_to_batch_size() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    ProducerOptions = kafine_producer_options:validate_options(#{
        linger_ms => 5,
        % Size of each message ends up being 17 bytes, + 61 byte overhead for batch. This holds 2 messages max
        max_batch_size_bytes => 100
    }),
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ProducerOptions),

    ReqIds1 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key1">>,
            value => <<"value1">>
        },
        test_label_1,
        kafine_producer:reqids_new()
    ),

    ReqIds2 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key2">>,
            value => <<"value2">>
        },
        test_label_2,
        ReqIds1
    ),

    % another produce is required to trigger max batch size
    ReqIds3 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key3">>,
            value => <<"value3">>
        },
        test_label_3,
        kafine_producer:reqids_new()
    ),

    % Request should complete for the two messages we could fit in a batch
    ?assertEqual(
        {
            ok,
            #{
                ?TOPIC_NAME => #{
                    ?PARTITION_1 => [{test_label_1, ok}, {test_label_2, ok}]
                }
            },
            kafine_producer:reqids_new()
        },
        kafine_producer:wait_all_responses(ReqIds2, ?WAIT_TIMEOUT_MS)
    ),

    % produce request should have had first two messages only
    ?assertCalled(kamock_produce, handle_produce_request, [
        #{
            topic_data => [
                #{
                    name => ?TOPIC_NAME,
                    partition_data => [
                        #{
                            index => ?PARTITION_1,
                            records => [
                                #{
                                    records => [
                                        #{
                                            key => <<"key1">>,
                                            value => <<"value1">>
                                        },
                                        #{
                                            key => <<"key2">>,
                                            value => <<"value2">>
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        '_'
    ]),

    % other produce should be produced by linger
    ?assertMatch(
        {{reply, ok}, test_label_3, _},
        kafine_producer:wait_response(ReqIds3, ?WAIT_TIMEOUT_MS)
    ),

    ?assertCalled(kamock_produce, handle_produce_request, [
        #{
            topic_data => [
                #{
                    name => ?TOPIC_NAME,
                    partition_data => [
                        #{
                            index => ?PARTITION_1,
                            records => [
                                #{
                                    records => [
                                        #{
                                            key => <<"key3">>,
                                            value => <<"value3">>
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        '_'
    ]),

    kafine:stop_producer(?PRODUCER_REF),
    ok.

produces_multiple_batches_due_to_max_size() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    ProducerOptions = kafine_producer_options:validate_options(#{
        % Long linger which won't trigger a produce
        linger_ms => 60_000,
        % Size of each message ends up being 17 bytes, + 61 byte overhead for batch. This holds 2 messages max
        max_batch_size_bytes => 100
    }),
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ProducerOptions),

    ReqIds1 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key1">>,
            value => <<"value1">>,
            headers => []
        },
        test_label_1,
        kafine_producer:reqids_new()
    ),

    ReqIds2 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key2">>,
            value => <<"value2">>,
            headers => []
        },
        test_label_2,
        ReqIds1
    ),

    ReqIds3 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key3">>,
            value => <<"value3">>,
            headers => []
        },
        test_label_3,
        ReqIds2
    ),

    ReqIds4 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key4">>,
            value => <<"value4">>,
            headers => []
        },
        test_label_4,
        ReqIds3
    ),

    % another produce is required to trigger max batch size
    _ = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key5">>,
            value => <<"value5">>,
            headers => []
        },
        test_label_5,
        kafine_producer:reqids_new()
    ),

    % Request should complete for the first 4 messages (produced as 2 batches)
    ?assertEqual(
        {
            ok,
            #{
                ?TOPIC_NAME => #{
                    ?PARTITION_1 => [
                        {test_label_1, ok},
                        {test_label_2, ok},
                        {test_label_3, ok},
                        {test_label_4, ok}
                    ]
                }
            },
            kafine_producer:reqids_new()
        },
        kafine_producer:wait_all_responses(ReqIds4, ?WAIT_TIMEOUT_MS)
    ),

    % Should be 2 produce requests of 2 records each
    ?assertCalled(kamock_produce, handle_produce_request, [
        #{
            topic_data => [
                #{
                    name => ?TOPIC_NAME,
                    partition_data => [
                        #{
                            index => ?PARTITION_1,
                            records => [
                                #{
                                    records => [
                                        #{
                                            key => <<"key1">>,
                                            value => <<"value1">>
                                        },
                                        #{
                                            key => <<"key2">>,
                                            value => <<"value2">>
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        '_'
    ]),

    ?assertCalled(kamock_produce, handle_produce_request, [
        #{
            topic_data => [
                #{
                    name => ?TOPIC_NAME,
                    partition_data => [
                        #{
                            index => ?PARTITION_1,
                            records => [
                                #{
                                    records => [
                                        #{
                                            key => <<"key3">>,
                                            value => <<"value3">>
                                        },
                                        #{
                                            key => <<"key4">>,
                                            value => <<"value4">>
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        '_'
    ]),

    kafine:stop_producer(?PRODUCER_REF),
    ok.

creates_telemetry() ->
    {ok, Broker = #{node_id := NodeId}} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    telemetry_test:attach_event_handlers(self(), [
        [kafine, producer, refresh_metadata],
        [kafine, producer, produce],
        [kafine, producer, request],
        [kafine, producer, response]
    ]),

    Ref = ?PRODUCER_REF,
    ProducerOptions = maps:merge(?PRODUCER_OPTIONS, #{metadata => #{test_meta => test_meta_value}}),
    {ok, _} = kafine:start_producer(Ref, Broker, ?CONNECTION_OPTIONS, ProducerOptions),

    TopicName = ?TOPIC_NAME,
    ?assertEqual(
        ok,
        kafine_producer:produce_sync(Ref, TopicName, ?PARTITION_1, #{
            key => <<"key">>,
            value => <<"value">>
        })
    ),

    ExpectedBroker = maps:with([host, port, node_id], Broker),
    ?assertReceived(
        {[kafine, producer, refresh_metadata], _, #{}, #{
            ref := Ref,
            test_meta := test_meta_value,
            topics := [TopicName],
            brokers := [ExpectedBroker]
        }}
    ),

    ?assertReceived(
        {[kafine, producer, produce], _, #{}, #{
            ref := Ref,
            test_meta := test_meta_value,
            topic := TopicName,
            partition := ?PARTITION_1
        }}
    ),

    ?assertReceived(
        {
            [kafine, producer, request],
            _,
            #{
                uncompressed_size_bytes := 76,
                message_count := 1,
                partition_count := 1
            },
            #{
                ref := Ref,
                test_meta := test_meta_value,
                leader_id := NodeId
            }
        }
    ),

    ?assertReceived(
        {
            [kafine, producer, response],
            _,
            #{
                batch_age_us := _
            },
            #{
                ref := Ref,
                test_meta := test_meta_value,
                topic := TopicName,
                partition := ?PARTITION_1,
                response := ok
            }
        }
    ),

    kafine:stop_producer(?PRODUCER_REF),
    ok.

info() ->
    {ok, Broker = #{node_id := NodeId}} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    ProducerOptions = kafine_producer_options:validate_options(?PRODUCER_OPTIONS),
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ProducerOptions),

    TopicName = ?TOPIC_NAME,
    ok = kafine_producer:produce_sync(?PRODUCER_REF, TopicName, ?PARTITION_1, #{}),

    ?assertMatch(
        #{
            producer_options := ProducerOptions,
            partitions := #{TopicName := #{?PARTITION_1 := #{leader_id := NodeId, state := ready}}},
            accumulator := _,
            node_producers := #{NodeId := _}
        },
        kafine_producer:info(?PRODUCER_REF)
    ),

    kafine:stop_producer(?PRODUCER_REF),
    ok.
