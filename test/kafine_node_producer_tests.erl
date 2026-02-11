-module(kafine_node_producer_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include("../src/kafine_eqwalizer.hrl").
-include("assert_meck.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(PRODUCER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CONNECTION_OPTIONS, #{}).

setup() ->
    meck:new(kafine_producer, [stub_all]),
    ok.

cleanup(_) ->
    meck:unload().

kafine_node_producer_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun simple_tests/0
    ]}.

simple_tests() ->
    {ok, Broker} = ?DYNAMIC_CAST(kamock_broker:start(?BROKER_REF)),

    {ok, Pid} = kafine_node_producer:start_link(?PRODUCER_REF, ?CONNECTION_OPTIONS, self(), Broker),

    {ok, #{error_code := ?NONE}} = kafine_node_producer:produce(
        Pid, ?TOPIC_NAME, 0, #{acks => full_isr}, #{}, [
            #{
                key => <<"key">>,
                value => <<"value">>,
                headers => []
            }
        ]
    ),

    ?assertCalled(kafine_producer, set_node_producer, [self(), Broker, Pid]),

    ok.
