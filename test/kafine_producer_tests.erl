-module(kafine_producer_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(PRODUCER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CONNECTION_OPTIONS, #{}).
-define(PARTITION_1, 1).
-define(PARTITION_2, 2).
-define(PARTITION_3, 3).

setup() ->
    kafine_producer_sup_sup:start_link(),
    meck:new(kamock_partition_produce_response, [passthrough]),
    meck:new(kamock_metadata, [passthrough]),
    ok.

cleanup(_) ->
    kafine_producer_sup_sup:stop(),
    meck:unload().

kafine_node_producer_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun simple_tests/0,
        fun batch_produce/0,
        fun batch_produce_with_errors/0,
        fun will_refresh_metadata_on_not_leader_error/0,
        fun will_retry_on_error/0,
        fun will_retry_up_to_max/0
    ]}.

simple_tests() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS),

    {ok, #{error_code := ?NONE}} = kafine_producer:produce(
        ?PRODUCER_REF, ?TOPIC_NAME, ?PARTITION_1, #{}, #{}, [
            #{
                key => <<"key">>,
                value => <<"value">>,
                headers => []
            }
        ]
    ),

    kafine:stop_producer(?PRODUCER_REF),
    ok.

batch_produce() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS),

    {ok, Responses} = kafine_producer:produce_batch(?PRODUCER_REF, #{}, make_batch(?TOPIC_NAME), #{}),

    ExpectedResponses = #{
        ?PARTITION_1 =>
            #{
                index => ?PARTITION_1,
                error_code => ?NONE,
                base_offset => 0,
                error_message => null,
                log_append_time_ms => 0,
                log_start_offset => 0,
                record_errors => []
            },
        ?PARTITION_2 =>
            #{
                index => ?PARTITION_2,
                error_code => ?NONE,
                base_offset => 0,
                error_message => null,
                log_append_time_ms => 0,
                log_start_offset => 0,
                record_errors => []
            },
        ?PARTITION_3 =>
            #{
                index => ?PARTITION_3,
                error_code => ?NONE,
                base_offset => 0,
                error_message => null,
                log_append_time_ms => 0,
                log_start_offset => 0,
                record_errors => []
            }
    },
    ?assertMatch(ExpectedResponses, maps:get(?TOPIC_NAME, Responses)),
    ok.

batch_produce_with_errors() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS),

    IsPartition = fun(PartitionIndex) -> fun(#{index := P}) -> PartitionIndex =:= P end end,
    meck:expect(
        kamock_partition_produce_response,
        make_partition_produce_response,
        [
            {
                ['_', meck:is(IsPartition(?PARTITION_3)), '_'],
                fail_produce_response(?CORRUPT_MESSAGE)
            },
            {['_', '_', '_'], fun pass_produce_response/3}
        ]
    ),

    {ok, Responses} = kafine_producer:produce_batch(?PRODUCER_REF, #{}, make_batch(?TOPIC_NAME), #{}),

    ExpectedResponses = #{
        ?PARTITION_1 =>
            #{
                index => ?PARTITION_1,
                error_code => ?NONE,
                base_offset => 0,
                error_message => null,
                log_append_time_ms => 0,
                log_start_offset => 0,
                record_errors => []
            },
        ?PARTITION_2 =>
            #{
                index => ?PARTITION_2,
                error_code => ?NONE,
                base_offset => 0,
                error_message => null,
                log_append_time_ms => 0,
                log_start_offset => 0,
                record_errors => []
            },
        ?PARTITION_3 =>
            #{
                index => ?PARTITION_3,
                error_code => ?CORRUPT_MESSAGE,
                base_offset => 0,
                error_message => null,
                log_append_time_ms => 0,
                log_start_offset => 0,
                record_errors => []
            }
    },
    ?assertMatch(ExpectedResponses, maps:get(?TOPIC_NAME, Responses)),
    ok.

make_batch(Topic) ->
    #{
        Topic => #{
            ?PARTITION_1 => [
                #{
                    key => <<"key">>,
                    value => <<"value">>,
                    headers => []
                }
            ],
            ?PARTITION_2 => [
                #{
                    key => <<"key">>,
                    value => <<"value">>,
                    headers => []
                }
            ],
            ?PARTITION_3 => [
                #{
                    key => <<"key">>,
                    value => <<"value">>,
                    headers => []
                }
            ]
        }
    }.

will_refresh_metadata_on_not_leader_error() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS),

    meck:expect(
        kamock_partition_produce_response,
        make_partition_produce_response,
        ['_', '_', '_'],
        meck:seq([
            % First call: fail with ERROR_NOT_LEADER_OR_FOLLOWER
            fail_produce_response(?NOT_LEADER_OR_FOLLOWER),
            % Otherwise, passthrough.
            meck:passthrough()
        ])
    ),

    % eventually produce succeeds
    {ok, #{error_code := ?NONE}} = kafine_producer:produce(
        ?PRODUCER_REF, ?TOPIC_NAME, ?PARTITION_1, #{}, #{}, [
            #{
                key => <<"key">>,
                value => <<"value">>,
                headers => []
            }
        ]
    ),

    History = meck:history(kamock_metadata),
    ?assertEqual(length(History), 2),

    History1 = meck:history(kamock_partition_produce_response),
    ?assertEqual(length(History1), 2),

    ok.

will_retry_on_error() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS),

    meck:expect(
        kamock_partition_produce_response,
        make_partition_produce_response,
        ['_', '_', '_'],
        meck:seq([
            % First call: fail with ERROR_NOT_LEADER_OR_FOLLOWER
            fail_produce_response(?REQUEST_TIMED_OUT),
            fail_produce_response(?NOT_ENOUGH_REPLICAS),
            fail_produce_response(?THROTTLING_QUOTA_EXCEEDED),
            % Otherwise, passthrough.
            meck:passthrough()
        ])
    ),

    RetryConfig = #{
        max_retries => 3,
        initial_backoff_ms => 0,
        multiplier => 10
    },

    % eventually produce succeeds
    {ok, #{error_code := ?NONE}} = kafine_producer:produce(
        ?PRODUCER_REF, ?TOPIC_NAME, ?PARTITION_1, RetryConfig, #{}, [
            #{
                key => <<"key">>,
                value => <<"value">>,
                headers => []
            }
        ]
    ),

    History = meck:history(kamock_partition_produce_response),
    ?assertEqual(length(History), 4),

    ok.

will_retry_up_to_max() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, _} = kafine:start_producer(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS),

    meck:expect(
        kamock_partition_produce_response,
        make_partition_produce_response,
        ['_', '_', '_'],
        meck:seq([
            % First call: fail with ERROR_NOT_LEADER_OR_FOLLOWER
            fail_produce_response(?REQUEST_TIMED_OUT),
            fail_produce_response(?NOT_ENOUGH_REPLICAS),
            fail_produce_response(?THROTTLING_QUOTA_EXCEEDED),
            % Otherwise, passthrough.
            meck:passthrough()
        ])
    ),
    MaxRetries = 1,
    RetryConfig = #{
        max_retries => MaxRetries,
        initial_backoff_ms => 0,
        multiplier => 10
    },

    % produce will fail after one attempt
    {ok, #{error_code := ?NOT_ENOUGH_REPLICAS}} = kafine_producer:produce(
        ?PRODUCER_REF, ?TOPIC_NAME, ?PARTITION_1, RetryConfig, #{}, [
            #{
                key => <<"key">>,
                value => <<"value">>,
                headers => []
            }
        ]
    ),


    History = meck:history(kamock_partition_produce_response),
    % Initial Request + MaxRetries
    ?assertEqual(length(History), MaxRetries + 1),

    ok.

fail_produce_response(ErrorCode) ->
    fun(_Topic, _Partition = #{index := PartitionIndex}, _Env) ->
        #{
            index => PartitionIndex,
            error_code => ErrorCode,
            error_message => null,
            base_offset => 0,
            log_start_offset => 0,
            log_append_time_ms => 0,
            record_errors => []
        }
    end.

pass_produce_response(_Topic, #{index := PartitionIndex}, _Env) ->
    #{
        index => PartitionIndex,
        error_code => ?NONE,
        error_message => null,
        base_offset => 0,
        log_start_offset => 0,
        log_append_time_ms => 0,
        record_errors => []
    }.
