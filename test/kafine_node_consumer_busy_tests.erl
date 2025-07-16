-module(kafine_node_consumer_busy_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    kafine_node_consumer_tests:setup(?MODULE).

cleanup(_) ->
    meck:unload().

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun all_busy/0
    ]}.

%% If all the consumer callbacks are busy, then we should skip the fetch.
all_busy() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            ?PARTITION_1 => #{state => active, offset => 0},
            ?PARTITION_2 => #{state => active, offset => 0}
        }
    }),

    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, node_consumer, idle],
        [kafine, node_consumer, fetch],
        [kafine, node_consumer, continue]
    ]),

    meck:expect(kamock_partition_data, make_partition_data, kamock_partition_data:empty()),

    % The consumer callback should take a while to process the records; it's busy.
    Sem = {n, l, make_ref()},
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _O, _I, St) ->
        % Wait until we're released.
        gproc:await(Sem),
        {ok, St}
    end),

    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % Wait for idle, fetch, idle.
    receive
        {[kafine, node_consumer, idle], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, node_consumer, fetch], TelemetryRef, _, _} -> ok
    end,
    receive
        {[kafine, node_consumer, idle], TelemetryRef, _, _} -> ok
    end,

    % We should see the first fetch to both partitions, then nothing.
    ?assertEqual(
        [{?PARTITION_1, 0}, {?PARTITION_2, 0}],
        fetch_request_history()
    ),

    ?assertMatch({idle, _}, sys:get_state(Pid)),

    % Release the callbacks.
    gproc:reg(Sem),

    % We should see some more fetches. We wait for end_record_batch, rather than look at fetch_request_history, because
    % the delays caused everything to get out of step.
    meck:wait(2, test_consumer_callback, end_record_batch, ['_', ?PARTITION_1, '_', '_', '_'], ?WAIT_TIMEOUT_MS),
    meck:wait(2, test_consumer_callback, end_record_batch, ['_', ?PARTITION_2, '_', '_', '_'], ?WAIT_TIMEOUT_MS),

    telemetry:detach(TelemetryRef),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

start_node_consumer(Ref, Broker, TopicPartitionStates) ->
    kafine_node_consumer_tests:start_node_consumer(Ref, Broker, TopicPartitionStates).

stop_node_consumer(Pid, TopicPartitionStates) ->
    kafine_node_consumer_tests:stop_node_consumer(Pid, TopicPartitionStates).

fetch_request_history() ->
    lists:sort(
        lists:filtermap(
            fun
                ({_, {_, make_partition_data, [_, #{partition := P, fetch_offset := O}, _]}, _}) ->
                    {true, {P, O}};
                (_) ->
                    false
            end,
            meck:history(kamock_partition_data)
        )
    ).
