-module(kafine_node_consumer_subscribe_tests).
-include_lib("eunit/include/eunit.hrl").

-include("src/consumer/kafine_topic_partition_state.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(PARTITION_3, 63).
-define(CALLBACK_STATE, ?MODULE).
-define(WAIT_TIMEOUT_MS, 2_000).
-define(CONNECTION_OPTIONS, #{}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun subscribe/0,
        fun unsubscribe/0,
        fun unsubscribe_all/0,
        fun unsubscribe_unknown/0
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

subscribe() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, NodeConsumer} = kafine_node_consumer:start_link(
        Broker,
        ?CONNECTION_OPTIONS,
        _ConsumerOptions = kafine_consumer_options:validate_options(#{}),
        {test_consumer_callback, undefined},
        self()
    ),

    % We should be in the 'idle' state -- we've got nothing to do, so we don't bother issuing empty fetches.
    ?assertMatch({idle, _}, sys:get_state(NodeConsumer)),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = #{
        TopicName => #{
            ?PARTITION_1 => #topic_partition_state{state = active, offset = 0},
            ?PARTITION_2 => #topic_partition_state{state = active, offset = 0}
        }
    },
    TopicOptions = #{TopicName => kafine_topic_options:validate_options(#{})},
    kafine_node_consumer:subscribe(NodeConsumer, TopicPartitionStates, TopicOptions),

    % We should be in one of the active states -- we've got things to do.
    ?assertNotMatch({idle, _}, sys:get_state(NodeConsumer)),

    % We should see a fetch.
    meck:wait(kamock_fetch, handle_fetch_request, '_', ?WAIT_TIMEOUT_MS),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, '_', '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, '_', '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    kafine_node_consumer:stop(NodeConsumer),
    kamock_broker:stop(Broker),
    ok.

unsubscribe() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, NodeConsumer} = kafine_node_consumer:start_link(
        Broker,
        ?CONNECTION_OPTIONS,
        _ConsumerOptions = kafine_consumer_options:validate_options(#{}),
        {test_consumer_callback, undefined},
        self()
    ),

    % We should be in the 'idle' state -- we've got nothing to do, so we don't bother issuing empty fetches.
    ?assertMatch({idle, _}, sys:get_state(NodeConsumer)),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = #{
        TopicName => #{
            ?PARTITION_1 => #topic_partition_state{state = active, offset = 0},
            ?PARTITION_2 => #topic_partition_state{state = active, offset = 0}
        }
    },
    TopicOptions = #{TopicName => kafine_topic_options:validate_options(#{})},
    kafine_node_consumer:subscribe(NodeConsumer, TopicPartitionStates, TopicOptions),

    % We should be in one of the active states -- we've got things to do.
    ?assertNotMatch({idle, _}, sys:get_state(NodeConsumer)),

    % We should see a fetch.
    meck:wait(kamock_fetch, handle_fetch_request, '_', ?WAIT_TIMEOUT_MS),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, '_', '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, '_', '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % ^^ this is the same as the previous test; now we unsubscribe from one of the partitions.
    kafine_node_consumer:unsubscribe(NodeConsumer, #{TopicName => [?PARTITION_1]}),

    % We should be in one of the active states -- we've got things to do.
    ?assertNotMatch({idle, _}, sys:get_state(NodeConsumer)),

    meck:wait(kamock_fetch, handle_fetch_request, '_', ?WAIT_TIMEOUT_MS),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, '_', '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, '_', '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % Unsubscribe from the other partition.
    kafine_node_consumer:unsubscribe(NodeConsumer, #{TopicName => [?PARTITION_2]}),

    % There's a (sorta) race here; if there's a fetch in flight, we don't go 'idle' until it completes.
    %
    % But: we can't wait for a fetch request at the mock broker, because we _might_ just squeak in ahead of sending one.

    % So we're gonna cheat. We'll sleep for _just_ long enough for any fetch to complete. The default max_wait_ms is
    % 500ms, but the mock broker divides this by 10 (so 50ms). Add 50% to be on the safe side.
    timer:sleep(75),
    ?assertMatch({idle, _}, sys:get_state(NodeConsumer)),

    kafine_node_consumer:stop(NodeConsumer),
    kamock_broker:stop(Broker),
    ok.

unsubscribe_all() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, NodeConsumer} = kafine_node_consumer:start_link(
        Broker,
        ?CONNECTION_OPTIONS,
        _ConsumerOptions = kafine_consumer_options:validate_options(#{}),
        {test_consumer_callback, undefined},
        self()
    ),

    % We should be in the 'idle' state -- we've got nothing to do, so we don't bother issuing empty fetches.
    ?assertMatch({idle, _}, sys:get_state(NodeConsumer)),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = #{
        TopicName => #{
            ?PARTITION_1 => #topic_partition_state{state = active, offset = 0},
            ?PARTITION_2 => #topic_partition_state{state = active, offset = 0}
        }
    },
    TopicOptions = #{TopicName => kafine_topic_options:validate_options(#{})},
    kafine_node_consumer:subscribe(NodeConsumer, TopicPartitionStates, TopicOptions),

    % We should be in one of the active states -- we've got things to do.
    ?assertNotMatch({idle, _}, sys:get_state(NodeConsumer)),

    % We should see a fetch.
    meck:wait(kamock_fetch, handle_fetch_request, '_', ?WAIT_TIMEOUT_MS),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, '_', '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, '_', '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % ^^ this is the same as the previous test; However, now we unsubscribe from all of the partitions.
    kafine_node_consumer:unsubscribe_all(NodeConsumer),

    timer:sleep(75),
    ?assertMatch({idle, _}, sys:get_state(NodeConsumer)),

    kafine_node_consumer:stop(NodeConsumer),
    kamock_broker:stop(Broker),
    ok.

unsubscribe_unknown() ->
    % kafine_node_consumer should ignore unsubscribe requests for a topic/partition that it knows nothing about. But,
    % just to be annoying, we'll mix this up by subscribing to two partitions, then unsubscribing from one of those,
    % plus one unknown.
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, NodeConsumer} = kafine_node_consumer:start_link(
        Broker,
        ?CONNECTION_OPTIONS,
        _ConsumerOptions = kafine_consumer_options:validate_options(#{}),
        {test_consumer_callback, undefined},
        self()
    ),

    % We should be in the 'idle' state -- we've got nothing to do, so we don't bother issuing empty fetches.
    ?assertMatch({idle, _}, sys:get_state(NodeConsumer)),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = #{
        TopicName => #{
            ?PARTITION_1 => #topic_partition_state{state = active, offset = 0},
            ?PARTITION_2 => #topic_partition_state{state = active, offset = 0}
        }
    },
    TopicOptions = #{TopicName => kafine_topic_options:validate_options(#{})},
    kafine_node_consumer:subscribe(NodeConsumer, TopicPartitionStates, TopicOptions),

    % We've subscribed.
    ?assertMatch(
        #{
            topic_partitions := #{
                TopicName := #{
                    ?PARTITION_1 := #{offset := 0, state := active},
                    ?PARTITION_2 := #{offset := 0, state := active}
                }
            }
        },
        kafine_node_consumer:info(NodeConsumer)
    ),

    % Unsubscribe.
    kafine_node_consumer:unsubscribe(NodeConsumer, #{TopicName => [?PARTITION_1, ?PARTITION_3]}),
    ?assertMatch(
        #{
            topic_partitions := #{
                TopicName := #{
                    ?PARTITION_2 := #{offset := 0, state := active}
                }
            }
        },
        kafine_node_consumer:info(NodeConsumer)
    ),

    kafine_node_consumer:stop(NodeConsumer),
    kamock_broker:stop(Broker),
    ok.
