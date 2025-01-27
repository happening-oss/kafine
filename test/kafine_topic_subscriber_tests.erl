-module(kafine_topic_subscriber_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(WAIT_TIMEOUT_MS, 2_000).
-define(SUBSCRIBER_REF, subscriber).
-define(CALLBACK_STATE, undefined).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun subscription_callback_is_called/0
    ]}.

setup() ->
    meck:new(test_subscription_callback, [non_strict]),
    meck:expect(test_subscription_callback, init, fun(_) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_subscription_callback, subscribe_partitions, fun(_, _, St) -> {ok, St} end),
    meck:expect(test_subscription_callback, unsubscribe_partitions, fun(St) -> {ok, St} end),

    meck:new(test_assignment_callback, [non_strict]),
    meck:expect(test_assignment_callback, init, fun(_) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_assignment_callback, before_assignment, fun(_, _, St) -> {ok, St} end),
    meck:expect(test_assignment_callback, after_assignment, fun(_, _, St) -> {ok, St} end),

    meck:new(kamock_fetch, [passthrough]),
    meck:new(kamock_metadata, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

subscription_callback_is_called() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    Topics = [?TOPIC_NAME],
    {ok, _R} = kafine_topic_subscriber:start_link(
        ?SUBSCRIBER_REF,
        Broker,
        #{},
        #{
            subscription_callback => {test_subscription_callback, undefined},
            assignment_callback => {test_assignment_callback, undefined}
        },
        Topics
    ),

    timer:sleep(1000),

    meck:wait(
        kamock_metadata,
        handle_metadata_request,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    meck:wait(
        test_subscription_callback,
        subscribe_partitions,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    ExpectedAssignment = #{?TOPIC_NAME => [0, 1, 2, 3]},

    ?assert(
        meck:called(test_subscription_callback, init, '_')
    ),
    ?assert(
        meck:called(test_subscription_callback, subscribe_partitions, [
            '_',
            meck:is(fun(Map) when Map =:= ExpectedAssignment ->
                true
            end),
            '_'
        ])
    ),

    ?assert(
        meck:called(test_assignment_callback, init, '_')
    ),
    ?assert(
        meck:called(test_assignment_callback, before_assignment, '_')
    ),
    ?assert(
        meck:called(test_assignment_callback, after_assignment, '_')
    ),


    kamock_broker:stop(Broker),
    ok.
