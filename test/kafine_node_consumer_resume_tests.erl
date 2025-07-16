-module(kafine_node_consumer_resume_tests).
-include_lib("eunit/include/eunit.hrl").

-include("history_matchers.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    kafine_node_consumer_tests:setup(?MODULE).

cleanup(_) ->
    meck:unload().

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun start_paused_resume_later/0,
        fun resume_unknown_topic_partition/0,
        fun resume_from_offset/0
    ]}.

start_paused_resume_later() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % ConsumerCallback:init is called by kafine_consumer, not kafine_node_consumer, so this doesn't actually get
    % invoked. But maybe it's more readable if we pretend.
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {pause, ?CALLBACK_STATE} end),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{state => paused},
            ?PARTITION_2 => #{state => paused}
        }
    }),

    % Pretend that there are some messages.
    kafine_kamock:produce(0, 4),

    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),
    ?assertMatch({idle, _}, sys:get_state(Pid)),

    ok = kafine_node_consumer:resume(Pid, TopicName, ?PARTITION_1),

    % We should start seeing messages from partition 1, but not the other.
    ?assertNotMatch({idle, _}, sys:get_state(Pid)),

    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    ?assertMatch(
        [
            ?init_callback(TopicName, ?PARTITION_1, ?CALLBACK_ARGS),
            ?init_callback(TopicName, ?PARTITION_2, ?CALLBACK_ARGS),
            ?begin_record_batch(TopicName, ?PARTITION_1, 0, 0, 4, 4),
            ?handle_record(TopicName, ?PARTITION_1, 0, _, _),
            ?end_record_batch(TopicName, ?PARTITION_1, 1, 0, 4, 4),
            ?begin_record_batch(TopicName, ?PARTITION_1, 1, 0, 4, 4),
            ?handle_record(TopicName, ?PARTITION_1, 1, _, _),
            ?end_record_batch(TopicName, ?PARTITION_1, 2, 0, 4, 4)
        ],
        meck:history(test_consumer_callback)
    ),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

resume_from_offset() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {pause, ?CALLBACK_STATE} end),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{state => paused}
        }
    }),

    % Pretend that there are some messages.
    kafine_kamock:produce(0, 4),

    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),
    ?assertMatch({idle, _}, sys:get_state(Pid)),

    ok = kafine_node_consumer:resume(Pid, TopicName, ?PARTITION_1, 2),

    % We should start seeing messages from partition 1 starting with offset 2
    ?assertNotMatch({idle, _}, sys:get_state(Pid)),

    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    ?assertMatch(
        [
            ?init_callback(TopicName, ?PARTITION_1, ?CALLBACK_ARGS),
            ?begin_record_batch(TopicName, ?PARTITION_1, 2, 0, 4, 4),
            ?handle_record(TopicName, ?PARTITION_1, 2, _, _),
            ?end_record_batch(TopicName, ?PARTITION_1, 3, 0, 4, 4),
            ?begin_record_batch(TopicName, ?PARTITION_1, 3, 0, 4, 4),
            ?handle_record(TopicName, ?PARTITION_1, 3, _, _),
            ?end_record_batch(TopicName, ?PARTITION_1, 4, 0, 4, 4)
        ],
        meck:history(test_consumer_callback)
    ),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

resume_unknown_topic_partition() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % ConsumerCallback:init is called by kafine_consumer, not kafine_node_consumer, so this doesn't actually get
    % invoked. But maybe it's more readable if we pretend.
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {pause, ?CALLBACK_STATE} end),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{state => paused}
        }
    }),

    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),
    ?assertMatch({idle, _}, sys:get_state(Pid)),

    ?assertMatch(
        {error, {badkey, [TopicName, ?PARTITION_2]}},
        kafine_node_consumer:resume(Pid, TopicName, ?PARTITION_2)
    ),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

start_node_consumer(Ref, Broker, TopicPartitionStates) ->
    kafine_node_consumer_tests:start_node_consumer(Ref, Broker, TopicPartitionStates).

stop_node_consumer(Pid, TopicPartitionStates) ->
    kafine_node_consumer_tests:stop_node_consumer(Pid, TopicPartitionStates).
