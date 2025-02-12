-module(kafine_node_consumer_tests).
-include_lib("eunit/include/eunit.hrl").
% Used by other tests.
-export([
    start_node_consumer/1,
    start_node_consumer/2
]).

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_STATE, ?MODULE).
-define(WAIT_TIMEOUT_MS, 2_000).
-define(CONNECTION_OPTIONS, #{}).

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    meck:expect(kafine_consumer, init_ack, fun(_Ref, _Topic, _Partition, _State) -> ok end),

    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload().

kafine_node_consumer_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun empty_topic/0,
        fun empty_topic_repeated_fetch/0,
        % tests where messages are actually returned (in various combinations) are in kafine_node_consumer_fetch_tests.erl
        fun offset_out_of_range/0
    ]}.

start_node_consumer(Broker) ->
    % validate_options is a helper function; we only call it because we're testing kafine_node_consumer directly.
    ConsumerOptions = kafine_consumer_options:validate_options(#{}),
    kafine_node_consumer:start_link(
        Broker,
        ?CONNECTION_OPTIONS,
        ConsumerOptions,
        self()
    ).

start_node_consumer(Broker, TopicPartitionStates) ->
    {ok, Pid} = start_node_consumer(Broker),
    TopicNames = maps:keys(TopicPartitionStates),
    TopicOptions =
        #{TopicName => kafine_topic_options:validate_options(#{}) || TopicName <- TopicNames},
    ok = kafine_node_consumer:subscribe(Pid, TopicPartitionStates, TopicOptions),
    {ok, Pid}.

empty_topic() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            61 => #{},
            62 => #{}
        }
    }),
    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % Wait for two calls to end_record_batch (2 partitions => 2 calls).
    meck:wait(
        2,
        test_consumer_callback,
        end_record_batch,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    kafine_node_consumer:stop(Pid),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

empty_topic_repeated_fetch() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            61 => #{}
        }
    }),
    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % Wait for two calls to end_record_batch, since we should repeat.
    meck:wait(
        2,
        test_consumer_callback,
        end_record_batch,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    kafine_node_consumer:stop(Pid),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

offset_out_of_range() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            % Note the -1 offset.
            61 => #{offset => -1}
        }
    }),
    {ok, Pid} = start_node_consumer(Broker, TopicPartitionStates),

    % The node consumer should issue ListOffsets, then Fetch, so we should see end_record_batch.
    meck:wait(kamock_list_offsets, handle_list_offsets_request, '_', ?WAIT_TIMEOUT_MS),
    meck:wait(kamock_fetch, handle_fetch_request, '_', ?WAIT_TIMEOUT_MS),

    meck:wait(test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    kafine_node_consumer:stop(Pid),
    cleanup_topic_partition_states(TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

cleanup_topic_partition_states(TopicPartitionStates) ->
    kafine_fetch_response_tests:cleanup_topic_partition_states(TopicPartitionStates).
