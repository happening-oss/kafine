-module(kafine_consumer_info_tests).
%%% Tests for the high-level API in kafine.erl
-include_lib("eunit/include/eunit.hrl").
-include("assert_received.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME_1, iolist_to_binary(io_lib:format("~s___~s_1_t", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_2_t", [?MODULE, ?FUNCTION_NAME]))).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(CONNECTION_OPTIONS, #{}).
-define(CONSUMER_OPTIONS, #{}).
-define(SUBSCRIBER_OPTIONS, #{}).
-define(FETCHER_METADATA, #{}).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun topic_consumer_info/0,
        fun group_consumer_info/0
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(kafine),

    meck:new(kamock_partition_data, [passthrough]),

    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),
    ok.

cleanup(_) ->
    meck:unload(),
    application:stop(kafine),
    ok.

topic_consumer_info() ->
    {ok, _Cluster, Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF),

    Topics = [?TOPIC_NAME_1, ?TOPIC_NAME_2],

    {ok, _Consumer} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        ?SUBSCRIBER_OPTIONS,
        #{
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS
        },
        Topics,
        #{},
        ?FETCHER_METADATA
    ),

    % Wait for init, a fetch from each topic partition should suffice
    meck:wait(8, kamock_partition_data, make_partition_data, '_', ?WAIT_TIMEOUT_MS),

    TopicName1 = ?TOPIC_NAME_1,
    TopicName2 = ?TOPIC_NAME_2,
    ExpectedConnectionOptions = kafine_connection_options:validate_options(?CONNECTION_OPTIONS),
    ExpectedTopicOptions = kafine_topic_options:validate_options(Topics, #{}),
    ExpectedBrokers = lists:map(fun(B) -> maps:with([node_id, host, port], B) end, Brokers),
    ?assertMatch(#{
        bootstrap := #{
            broker := Bootstrap,
            connection_options := ExpectedConnectionOptions,
            connection := _
        },
        metadata := #{
            table := _,
            brokers := ExpectedBrokers,
            partitions := #{
                TopicName1 := #{
                    0 := #{leader := 101, replicas := [101, 102, 103], isr := [101, 102, 103]},
                    1 := #{leader := 102, replicas := [102, 101, 103], isr := [102, 101, 103]},
                    2 := #{leader := 103, replicas := [103, 101, 102], isr := [103, 101, 102]},
                    3 := #{leader := 101, replicas := [101, 102, 103], isr := [101, 102, 103]}
                },
                TopicName2 := #{
                    0 := #{leader := 101, replicas := [101, 102, 103], isr := [101, 102, 103]},
                    1 := #{leader := 102, replicas := [102, 101, 103], isr := [102, 101, 103]},
                    2 := #{leader := 103, replicas := [103, 101, 102], isr := [103, 101, 102]},
                    3 := #{leader := 101, replicas := [101, 102, 103], isr := [101, 102, 103]}
                }
            }
        },
        fetcher := #{
            brokers := _,
            topic_partition_nodes := _
        },
        node_fetchers := _,
        parallel_subscription := #{
            topic_options := ExpectedTopicOptions,
            callback_mod := test_consumer_callback,
            callback_arg := ?CALLBACK_ARGS,
            children := _
        },
        topic_subscriber := #{
            topics := Topics,
            subscription_callback := {kafine_parallel_subscription_callback, ?CONSUMER_REF}
        }
    }, kafine_consumer:info(?CONSUMER_REF)).

group_consumer_info() ->
    telemetry_test:attach_event_handlers(self(), [[kafine, rebalance, stop]]),

    {ok, _Cluster, Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF),

    GroupId = ?GROUP_ID,
    Topics = [?TOPIC_NAME_1, ?TOPIC_NAME_2],

    {ok, _Consumer1} = kafine:start_group_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        ?CONNECTION_OPTIONS,
        GroupId,
        ?CONSUMER_OPTIONS,
        ?SUBSCRIBER_OPTIONS,
        #{
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS
        },
        Topics,
        #{},
        ?FETCHER_METADATA
    ),

    ?assertReceived({[kafine, rebalance, stop], _, _, _}),

    % Wait for init, a fetch from each topic partition should suffice
    meck:wait(8, kamock_partition_data, make_partition_data, '_', ?WAIT_TIMEOUT_MS),

    TopicName1 = ?TOPIC_NAME_1,
    TopicName2 = ?TOPIC_NAME_2,
    ExpectedConnectionOptions = kafine_connection_options:validate_options(?CONNECTION_OPTIONS),
    ExpectedTopicOptions = kafine_topic_options:validate_options(Topics, #{}),
    ExpectedBrokers = lists:map(fun(B) -> maps:with([node_id, host, port], B) end, Brokers),

    ?assertMatch(#{
        bootstrap := #{
            broker := Bootstrap,
            connection_options := ExpectedConnectionOptions,
            connection := _
        },
        metadata := #{
            table := _,
            brokers := ExpectedBrokers,
            partitions := #{
                TopicName1 := #{
                    0 := #{leader := 101, replicas := [101, 102, 103], isr := [101, 102, 103]},
                    1 := #{leader := 102, replicas := [102, 101, 103], isr := [102, 101, 103]},
                    2 := #{leader := 103, replicas := [103, 101, 102], isr := [103, 101, 102]},
                    3 := #{leader := 101, replicas := [101, 102, 103], isr := [101, 102, 103]}
                },
                TopicName2 := #{
                    0 := #{leader := 101, replicas := [101, 102, 103], isr := [101, 102, 103]},
                    1 := #{leader := 102, replicas := [102, 101, 103], isr := [102, 101, 103]},
                    2 := #{leader := 103, replicas := [103, 101, 102], isr := [103, 101, 102]},
                    3 := #{leader := 101, replicas := [101, 102, 103], isr := [101, 102, 103]}
                }
            }
        },
        fetcher := #{
            brokers := _,
            topic_partition_nodes := _
        },
        node_fetchers := _,
        parallel_subscription := #{
            topic_options := ExpectedTopicOptions,
            callback_mod := test_consumer_callback,
            callback_arg := ?CALLBACK_ARGS,
            children := _
        },
        coordinator := #{
            group_id := GroupId,
            topics := Topics,
            connection_options := ExpectedConnectionOptions,
            membership_options := _,
            broker := _,
            connection := _
        },
        eager_rebalance := #{
            group_id := GroupId,
            member_id := _,
            generation_id := _,
            membership_options := _,
            assignment := _,
            subscription_callback := {kafine_parallel_subscription_callback, ?CONSUMER_REF}
        }
    }, kafine_consumer:info(?CONSUMER_REF)).
