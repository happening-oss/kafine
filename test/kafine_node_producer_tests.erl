-module(kafine_node_producer_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_STATE, ?MODULE).
-define(WAIT_TIMEOUT_MS, 2_000).
-define(CONNECTION_OPTIONS, #{}).

setup() ->
    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload().

kafine_node_producer_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun simple_tests/0
    ]}.

simple_tests() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, Pid} = kafine_node_producer:start_link(Broker, ?CONNECTION_OPTIONS),

    {ok, #{error_code := ?NONE}} = kafine_node_producer:produce(Pid, ?TOPIC_NAME, 0, #{}, [
        #{
            key => <<"key">>,
            value => <<"value">>,
            headers => []
        }
    ]),

    ok.
