-module(kafine_consumer_pause_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, {state, ?MODULE}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun consumer_callback_init_pause/0
    ]}.

setup() ->
    meck:new(kamock_fetch, [passthrough]),
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

consumer_callback_init_pause() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {pause, ?CALLBACK_STATE} end),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    Topic1 = ?TOPIC_NAME,
    ok = kafine_consumer:subscribe(Consumer, #{Topic1 => {#{}, #{0 => 0}}}),

    % There should be one node consumer (because one broker); it should be idle.
    #{node_consumers := NodeConsumers} = kafine_consumer:info(Consumer),
    ?assertEqual(1, map_size(NodeConsumers)),
    maps:foreach(
        fun(_NodeId, Pid) ->
            ?assertMatch({idle, _}, sys:get_state(Pid))
        end,
        NodeConsumers
    ),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.
