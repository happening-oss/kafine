-module(kafine_node_consumer_fetch_batch_tests).
-include_lib("eunit/include/eunit.hrl").

-include("src/consumer/kafine_topic_partition_state.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(CALLBACK_STATE, ?MODULE).
-define(WAIT_TIMEOUT_MS, 2_000).
-define(CONNECTION_OPTIONS, #{}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun fetch_in_last_batch/0
    ]}.

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

fetch_in_last_batch() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = #{
        TopicName => #{
            ?PARTITION_1 => #topic_partition_state{state = active, offset = 12}
        }
    },

    MessageBuilder = fun(_T, _P, O) ->
        Key = iolist_to_binary(io_lib:format("key~B", [O])),
        #{key => Key}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 14, 3, MessageBuilder)
    ),

    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % We should reach parity at some point.

    % TODO: We don't really have a good way to detect parity with a meck matcher. Does that tell us that our API is a
    % bit suss?
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, 14, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_node_consumer:stop(Pid),
    kamock_broker:stop(Broker),
    ok.

start_node_consumer(Broker, TopicPartitionStates) ->
    % validate_options is a helper function; we only call it because we're testing kafine_node_consumer directly.
    ConsumerOptions = kafine_consumer_options:validate_options(#{}),
    TopicNames = maps:keys(TopicPartitionStates),
    TopicOptions =
        #{TopicName => kafine_topic_options:validate_options(#{}) || TopicName <- TopicNames},
    {ok, Pid} = kafine_node_consumer:start_link(
        Broker,
        ?CONNECTION_OPTIONS,
        ConsumerOptions,
        {test_consumer_callback, undefined},
        self()
    ),
    ok = kafine_node_consumer:subscribe(Pid, TopicPartitionStates, TopicOptions),
    {ok, Pid}.
