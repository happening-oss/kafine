-module(kafine_node_consumer_next_action_tests).
%%% The looping in kafine_node_consumer is kinda complicated. This test suite contains tests for that.

-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(PARTITION_3, 63).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun subscribe_while_fetching/0
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    meck:expect(kafine_consumer, init_ack, fun(_Ref, _Topic, _Partition, _State) -> ok end),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

%% What happens if you call 'subscribe' while a Fetch request is in flight?
%%
%% Note that this test is kinda coupled to the implementation of kafine_node_consumer. It's not ideal, but the internals
%% are quite complicated, so I feel justified in making that trade-off.
subscribe_while_fetching() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, NodeConsumer} = kafine_node_consumer_tests:start_node_consumer(?CONSUMER_REF, Broker),

    % We should be in the 'idle' state -- we've got nothing to do, so we don't bother issuing empty fetches.
    ?assertMatch({idle, _}, sys:get_state(NodeConsumer)),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{},
            ?PARTITION_2 => #{}
        }
    }),
    TopicOptions = #{TopicName => kafine_topic_options:validate_options(#{})},

    % Synchronise with the Fetch request.
    TestPid = self(),
    meck:expect(
        kamock_fetch,
        handle_fetch_request,
        ['_', '_'],
        meck:seq([
            % First call: synchronise on the test.
            fun(FetchRequest, Env) ->
                % Notify the test that we've received the Fetch request.
                TestPid ! {notify_fetch, self()},

                % Wait until the test releases us.
                receive
                    continue_fetch -> ok
                end,
                meck:passthrough([FetchRequest, Env])
            end,

            % Otherwise, passthrough.
            meck:passthrough()
        ])
    ),

    kafine_node_consumer:subscribe(NodeConsumer, TopicPartitionStates, TopicOptions),

    % Wait until the broker's handling the Fetch request.
    RequestPid =
        receive
            {notify_fetch, Pid} -> Pid
        end,

    % The node consumer should be waiting for a fetch response (it's in the 'fetch' state). This also asserts that the
    % node consumer process isn't blocked synchronously on the fetch.
    ?assertMatch({fetch, _}, sys:get_state(NodeConsumer)),

    % Subscribe to another partition.
    MoreTopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_3 => #{}
        }
    }),
    kafine_node_consumer:subscribe(NodeConsumer, MoreTopicPartitionStates, TopicOptions),

    % Release the broker.
    RequestPid ! continue_fetch,

    % We should see the initial fetch.
    [
        meck:wait(
            test_consumer_callback,
            end_record_batch,
            ['_', P, '_', '_', '_'],
            ?WAIT_TIMEOUT_MS
        )
     || P <- [?PARTITION_1, ?PARTITION_2]
    ],
    meck:reset(test_consumer_callback),

    % We should see some more fetches.
    [
        meck:wait(
            test_consumer_callback,
            end_record_batch,
            ['_', P, '_', '_', '_'],
            ?WAIT_TIMEOUT_MS
        )
     || P <- [?PARTITION_1, ?PARTITION_2, ?PARTITION_3]
    ],

    kafine_node_consumer:stop(NodeConsumer),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

cleanup_topic_partition_states(TopicPartitionStates) ->
    kafine_fetch_response_tests:cleanup_topic_partition_states(TopicPartitionStates).
