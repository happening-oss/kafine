-module(kafine_node_producer_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include("../src/kafine_eqwalizer.hrl").
-include("assert_meck.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(PRODUCER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_t_2", [?MODULE, ?FUNCTION_NAME]))).
-define(TIMESTAMP, 1_000_000_000).
-define(CONNECTION_OPTIONS, #{}).
-define(PRODUCER_OPTIONS, #{}).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    meck:new(kamock_produce, [passthrough]),
    meck:new(kafine_producer, [stub_all]),
    ok.

cleanup(_) ->
    meck:unload().

kafine_node_producer_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun produce_batch/0,
        fun produce_options/0,
        fun info/0
    ]}.

produce_batch() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, Pid} = kafine_node_producer:start_link(
        ?PRODUCER_REF, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS, self(), Broker
    ),

    Batch = #{
        ?TOPIC_NAME => #{
            0 => [make_message(N) || N <- lists:seq(1, 3)],
            1 => [make_message(N) || N <- lists:seq(4, 6)]
        },
        ?TOPIC_NAME_2 => #{
            2 => [make_message(N) || N <- lists:seq(7, 9)]
        }
    },
    ReqIds = kafine_node_producer:produce(
        Pid, Batch, test_label, kafine_node_producer:reqids_new()
    ),

    Result = wait_response(ReqIds),

    ExpectedResult = #{
        ?TOPIC_NAME => #{
            0 => ok,
            1 => ok
        },
        ?TOPIC_NAME_2 => #{
            2 => ok
        }
    },
    ?assertEqual({ExpectedResult, test_label, kafine_node_producer:reqids_new()}, Result).

produce_options() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    ConnectionOptions = #{request_timeout_ms => 12_345},
    ProducerOptions = #{acks => leader, compression => gzip},

    {ok, Pid} = kafine_node_producer:start_link(
        ?PRODUCER_REF, ConnectionOptions, ProducerOptions, self(), Broker
    ),

    Batch = #{?TOPIC_NAME => #{0 => [make_message(N) || N <- lists:seq(1, 3)]}},
    ReqIds = kafine_node_producer:produce(
        Pid, Batch, test_label, kafine_node_producer:reqids_new()
    ),

    _ = wait_response(ReqIds),

    ?assertCalled(kamock_produce, handle_produce_request, [
        meck:is(fun
            (
                #{
                    acks := 1,
                    timeout_ms := 12_345,
                    topic_data := [
                        #{
                            partition_data := [
                                #{
                                    records := [
                                        #{
                                            attributes := #{
                                                compression := gzip
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ) ->
                true;
            (_) ->
                false
        end),
        '_'
    ]).

info() ->
    {ok, Broker = #{node_id := NodeId}} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, Pid} = kafine_node_producer:start_link(
        ?PRODUCER_REF, ?CONNECTION_OPTIONS, ?PRODUCER_OPTIONS, self(), Broker
    ),

    ?assertMatch(
        #{
            state := ready,
            node_id := NodeId,
            broker := Broker,
            connection_options := _,
            producer_options := _,
            connection := _
        },
        kafine_node_producer:info(Pid)
    ).

make_message(N) ->
    #{
        timestamp => ?TIMESTAMP,
        key => iolist_to_binary(io_lib:format("key_~p", [N])),
        value => iolist_to_binary(io_lib:format("value_~p", [N])),
        headers => []
    }.

wait_response(ReqIds) ->
    % yes, gen_statem:wait_response exists, but this exercises the wrapper around check_response
    receive
        Msg ->
            case kafine_node_producer:check_response(Msg, ReqIds) of
                no_reply ->
                    error({unexpected_message, Msg});
                Result ->
                    Result
            end
    after ?WAIT_TIMEOUT_MS ->
        error(timeout)
    end.
