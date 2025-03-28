-module(kafine_consumer_offset_reset_policy_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
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

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun earliest/0,
        fun latest/0
    ]}.

earliest() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    % Initially, pretend to have 3 messages.
    kafine_kamock:produce(0, 3),

    % If we specify 'earliest', we should get all of the messages.
    TopicName = ?TOPIC_NAME,
    Partition = 0,
    Subscription = #{TopicName => {#{offset_reset_policy => earliest}, #{Partition => earliest}}},
    kafine_consumer:subscribe(Consumer, Subscription),

    meck:wait(3, test_consumer_callback, handle_record, '_', ?WAIT_TIMEOUT_MS),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key0">>}},
            {TopicName, Partition, #{key := <<"key1">>}},
            {TopicName, Partition, #{key := <<"key2">>}}
        ],
        received_records()
    ),
    meck:reset(test_consumer_callback),

    % "Produce" another 2 messages.
    kafine_kamock:produce(0, 5),

    meck:wait(2, test_consumer_callback, handle_record, '_', ?WAIT_TIMEOUT_MS),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key3">>}},
            {TopicName, Partition, #{key := <<"key4">>}}
        ],
        received_records()
    ),
    meck:reset(test_consumer_callback),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

latest() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    % Initially, pretend to have 3 messages.
    kafine_kamock:produce(0, 3),

    % If we specify 'latest', we should initially get no messages.
    TopicName = ?TOPIC_NAME,
    Partition = 0,
    Subscription = #{TopicName => {#{offset_reset_policy => latest}, #{Partition => latest}}},
    kafine_consumer:subscribe(Consumer, Subscription),

    meck:wait(1, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),
    ?assertMatch([], received_records()),
    meck:reset(test_consumer_callback),

    % "Produce" another 2 messages. We should get those.
    kafine_kamock:produce(0, 5),

    meck:wait(2, test_consumer_callback, handle_record, '_', ?WAIT_TIMEOUT_MS),
    ?assertMatch(
        [
            {TopicName, Partition, #{key := <<"key3">>}},
            {TopicName, Partition, #{key := <<"key4">>}}
        ],
        received_records()
    ),
    meck:reset(test_consumer_callback),

    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.

received_records() ->
    lists:filtermap(
        fun
            ({_, {_, handle_record, [TopicName, Partition, Record, _]}, _}) ->
                {true, {TopicName, Partition, Record}};
            (_) ->
                false
        end,
        meck:history(test_consumer_callback)
    ).
