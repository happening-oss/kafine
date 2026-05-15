-module(kafine_producer_message_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kafine_eqwalizer.hrl").
-include("assert_meck.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(PRODUCER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CONNECTION_OPTIONS, #{}).
-define(PRODUCER_OPTIONS, #{linger_ms => 0}).
-define(PARTITION_1, 1).
-define(WAIT_TIMEOUT_MS, 2_000).
-define(TIMESTAMP, 10_000_000).

setup() ->
    kafine_producer_sup_sup:start_link(),
    meck:new(kafine_producer, [passthrough]),
    meck:expect(kafine_producer, timestamp, fun() -> ?TIMESTAMP end),
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
        fun value_only/0,
        fun key_only/0,
        fun header_only/0,
        fun header_with_null_value/0,
        fun header_with_null_key_is_error/0,
        fun headers_with_duplicate_keys/0,
        fun multiple_records_have_correct_offset_deltas_and_timestamps/0
    ]}.

value_only() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    ?assertEqual(
        ok,
        kafine_producer:produce_sync(?PRODUCER_REF, ?TOPIC_NAME, ?PARTITION_1, #{
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
                                    base_timestamp => ?TIMESTAMP,
                                    max_timestamp => ?TIMESTAMP,
                                    records => [
                                        #{
                                            key => null,
                                            value => <<"value">>,
                                            headers => [],
                                            timestamp_delta => 0,
                                            offset_delta => 0
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

key_only() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    ?assertEqual(
        ok,
        kafine_producer:produce_sync(?PRODUCER_REF, ?TOPIC_NAME, ?PARTITION_1, #{
            key => <<"key">>
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
                                    base_timestamp => ?TIMESTAMP,
                                    max_timestamp => ?TIMESTAMP,
                                    records => [
                                        #{
                                            key => <<"key">>,
                                            value => null,
                                            headers => [],
                                            timestamp_delta => 0,
                                            offset_delta => 0
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

header_only() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    ?assertEqual(
        ok,
        kafine_producer:produce_sync(?PRODUCER_REF, ?TOPIC_NAME, ?PARTITION_1, #{
            headers => [{<<"header key">>, <<"header value">>}]
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
                                    base_timestamp => ?TIMESTAMP,
                                    max_timestamp => ?TIMESTAMP,
                                    records => [
                                        #{
                                            key => null,
                                            value => null,
                                            headers => [{<<"header key">>, <<"header value">>}],
                                            timestamp_delta => 0,
                                            offset_delta => 0
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

header_with_null_value() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    ?assertEqual(
        ok,
        kafine_producer:produce_sync(?PRODUCER_REF, ?TOPIC_NAME, ?PARTITION_1, #{
            headers => [{<<"header key">>, null}]
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
                                    base_timestamp => ?TIMESTAMP,
                                    max_timestamp => ?TIMESTAMP,
                                    records => [
                                        #{
                                            key => null,
                                            value => null,
                                            headers => [{<<"header key">>, null}],
                                            timestamp_delta => 0,
                                            offset_delta => 0
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

header_with_null_key_is_error() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    ?assertError(
        badarg,
        kafine_producer:produce_sync(
            ?PRODUCER_REF,
            ?TOPIC_NAME,
            ?PARTITION_1,
            ?DYNAMIC_CAST(#{
                headers => [{null, <<"header value">>}]
            })
        )
    ).

headers_with_duplicate_keys() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS),

    ?assertEqual(
        ok,
        kafine_producer:produce_sync(?PRODUCER_REF, ?TOPIC_NAME, ?PARTITION_1, #{
            headers => [
                {<<"key1">>, <<"value1">>}, {<<"key2">>, <<"value2">>}, {<<"key1">>, <<"value3">>}
            ]
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
                                    base_timestamp => ?TIMESTAMP,
                                    max_timestamp => ?TIMESTAMP,
                                    records => [
                                        #{
                                            key => null,
                                            value => null,
                                            headers => [
                                                {<<"key1">>, <<"value1">>},
                                                {<<"key2">>, <<"value2">>},
                                                {<<"key1">>, <<"value3">>}
                                            ],
                                            timestamp_delta => 0,
                                            offset_delta => 0
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

multiple_records_have_correct_offset_deltas_and_timestamps() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    ProducerOptions = #{linger_ms => 10},
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS, ProducerOptions),

    ReqIds1 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key1">>,
            timestamp => ?TIMESTAMP
        },
        undefined,
        kafine_producer:reqids_new()
    ),

    ReqIds2 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key2">>,
            timestamp => ?TIMESTAMP + 2
        },
        undefined,
        ReqIds1
    ),

    ReqIds3 = kafine_producer:produce(
        ?PRODUCER_REF,
        ?TOPIC_NAME,
        ?PARTITION_1,
        #{
            key => <<"key3">>,
            timestamp => ?TIMESTAMP + 1
        },
        undefined,
        ReqIds2
    ),

    ?assertMatch({ok, _, _}, kafine_producer:wait_all_responses(ReqIds3, ?WAIT_TIMEOUT_MS)),

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
                                    base_timestamp => ?TIMESTAMP,
                                    max_timestamp => ?TIMESTAMP + 2,
                                    records => [
                                        #{
                                            key => <<"key1">>,
                                            value => null,
                                            headers => [],
                                            timestamp_delta => 0,
                                            offset_delta => 0
                                        },
                                        #{
                                            key => <<"key2">>,
                                            value => null,
                                            headers => [],
                                            timestamp_delta => 2,
                                            offset_delta => 1
                                        },
                                        #{
                                            key => <<"key3">>,
                                            value => null,
                                            headers => [],
                                            timestamp_delta => 1,
                                            offset_delta => 2
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
