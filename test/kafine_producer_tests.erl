-module(kafine_producer_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(PRODUCER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CONNECTION_OPTIONS, #{}).

setup() ->
    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_partition_produce_response, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload().

kafine_node_producer_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun simple_tests/0,
        fun batch_produce/0,
        fun batch_produce_with_errors/0
    ]}.

simple_tests() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, Pid} = kafine_producer:start_link(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS),

    {ok, #{error_code := ?NONE}} = kafine_producer:produce(Pid, ?TOPIC_NAME, 0, #{}, [
        #{
            key => <<"key">>,
            value => <<"value">>,
            headers => []
        }
    ]),

    ok.

batch_produce() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, Pid} = kafine_producer:start_link(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS),

    {ok, Responses} = kafine_producer:produce_batch(Pid, make_batch(?TOPIC_NAME), #{}),

    ExpectedResponses = #{
        0 =>
            #{
                index => 0,
                error_code => 0,
                base_offset => 0,
                error_message => null,
                log_append_time_ms => 0,
                log_start_offset => 0,
                record_errors => []
            },
        1 =>
            #{
                index => 1,
                error_code => 0,
                base_offset => 0,
                error_message => null,
                log_append_time_ms => 0,
                log_start_offset => 0,
                record_errors => []
            },
        2 =>
            #{
                index => 2,
                error_code => 0,
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

    {ok, Pid} = kafine_producer:start_link(?PRODUCER_REF, Broker, ?CONNECTION_OPTIONS),

    meck:expect(
        kamock_partition_produce_response, make_partition_produce_response, fun fail_on_partition2/3
    ),

    {ok, Responses} = kafine_producer:produce_batch(Pid, make_batch(?TOPIC_NAME), #{}),

    ExpectedResponses = #{
        0 =>
            #{
                index => 0,
                error_code => 0,
                base_offset => 0,
                error_message => null,
                log_append_time_ms => 0,
                log_start_offset => 0,
                record_errors => []
            },
        1 =>
            #{
                index => 1,
                error_code => 0,
                base_offset => 0,
                error_message => null,
                log_append_time_ms => 0,
                log_start_offset => 0,
                record_errors => []
            },
        2 =>
            #{
                index => 2,
                error_code => ?NOT_LEADER_OR_FOLLOWER,
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
            0 => [
                #{
                    key => <<"key">>,
                    value => <<"value">>,
                    headers => []
                }
            ],
            1 => [
                #{
                    key => <<"key">>,
                    value => <<"value">>,
                    headers => []
                }
            ],
            2 => [
                #{
                    key => <<"key">>,
                    value => <<"value">>,
                    headers => []
                }
            ]
        }
    }.

fail_on_partition2(_Topic, #{index := 2}, _Env) ->
    #{
        index => 2,
        error_code => ?NOT_LEADER_OR_FOLLOWER,
        error_message => null,
        base_offset => 0,
        log_start_offset => 0,
        log_append_time_ms => 0,
        record_errors => []
    };
fail_on_partition2(_Topic, #{index := PartitionIndex}, _Env) ->
    #{
        index => PartitionIndex,
        error_code => ?NONE,
        error_message => null,
        base_offset => 0,
        log_start_offset => 0,
        log_append_time_ms => 0,
        record_errors => []
    }.
