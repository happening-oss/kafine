-module(kafine_metadata_cache_tests).

-include_lib("eunit/include/eunit.hrl").

-include("assert_meck.hrl").
-include("assert_received.hrl").

-define(REF, {?MODULE, ?FUNCTION_NAME}).
-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONNECTION_OPTIONS, #{client_id => <<"test_client">>}).
-define(TOPIC_NAME, <<"topic">>).
-define(TOPIC_NAME_2, <<"topic2">>).

setup() ->
    meck:new(kamock_metadata, [passthrough]),
    meck:new(kamock_metadata_response_topic, [passthrough]),
    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        fun
            (Topic = #{name := ?TOPIC_NAME}, Env) ->
                F = kamock_metadata_response_topic:partitions([0, 1]),
                F(Topic, Env);
            (Topic = #{name := ?TOPIC_NAME_2}, Env) ->
                F = kamock_metadata_response_topic:partitions([0, 1, 2]),
                F(Topic, Env)
        end
    ),
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

kafine_metadata_cache_test_() ->
    {foreach, spawn, fun setup/0, fun cleanup/1, [
        fun partitions/0,
        fun repeated_partitions_calls_only_fetch_once/0,
        fun partitions_for_multiple_topics/0,
        fun brokers/0,
        fun repeated_brokers_calls_only_fetch_once/0,
        fun brokers_does_not_fetch_if_brokers_exist_from_partitions_call/0,
        fun refresh_picks_up_updated_metadata/0,
        fun concurrent_refresh_attempts_only_fetch_once/0,
        fun concurrent_refresh_with_additional_topics_fetches_all_new_topics/0
    ]}.

partitions() ->
    {ok, _, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    {ok, _} = kafine_metadata_cache:start_link(?REF),

    Result = kafine_metadata_cache:partitions(?REF, ?TOPIC_NAME),

    ExpectedResult = #{
        ?TOPIC_NAME => #{
            0 => #{leader => 101, replicas => [101, 102], isr => [101, 102]},
            1 => #{leader => 102, replicas => [102, 101], isr => [102, 101]}
        }
    },

    ?assertEqual(ExpectedResult, Result).

repeated_partitions_calls_only_fetch_once() ->
    {ok, _, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    {ok, _} = kafine_metadata_cache:start_link(?REF),

    Result1 = kafine_metadata_cache:partitions(?REF, ?TOPIC_NAME),

    ExpectedResult = #{
        ?TOPIC_NAME => #{
            0 => #{leader => 101, replicas => [101, 102], isr => [101, 102]},
            1 => #{leader => 102, replicas => [102, 101], isr => [102, 101]}
        }
    },

    ?assertEqual(ExpectedResult, Result1),

    meck:reset(kamock_metadata),

    % query again
    Result2 = kafine_metadata_cache:partitions(?REF, ?TOPIC_NAME),

    % expect the same result
    ?assertEqual(ExpectedResult, Result2),

    % but we shouldn't have fetched the metadata again
    ?assertNotCalled(kamock_metadata, handle_metadata_request, '_').

partitions_for_multiple_topics() ->
    {ok, _, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    {ok, _} = kafine_metadata_cache:start_link(?REF),

    Result1 = kafine_metadata_cache:partitions(?REF, ?TOPIC_NAME),

    ExpectedResult1 = #{
        ?TOPIC_NAME => #{
            0 => #{leader => 101, replicas => [101, 102], isr => [101, 102]},
            1 => #{leader => 102, replicas => [102, 101], isr => [102, 101]}
        }
    },

    ?assertEqual(ExpectedResult1, Result1),

    % query a different topic
    Result2 = kafine_metadata_cache:partitions(?REF, ?TOPIC_NAME_2),

    ExpectedResult2 = #{
        ?TOPIC_NAME_2 => #{
            0 => #{leader => 101, replicas => [101, 102], isr => [101, 102]},
            1 => #{leader => 102, replicas => [102, 101], isr => [102, 101]},
            2 => #{leader => 101, replicas => [101, 102], isr => [101, 102]}
        }
    },

    ?assertEqual(ExpectedResult2, Result2),

    meck:reset(kamock_metadata),

    % query topics again
    Result3 = kafine_metadata_cache:partitions(?REF, ?TOPIC_NAME),
    ?assertEqual(ExpectedResult1, Result3),
    Result4 = kafine_metadata_cache:partitions(?REF, ?TOPIC_NAME_2),
    ?assertEqual(ExpectedResult2, Result4),

    ?assertNotCalled(kamock_metadata, handle_metadata_request, '_'),

    % query both topics together
    Result5 = kafine_metadata_cache:partitions(?REF, [?TOPIC_NAME, ?TOPIC_NAME_2]),
    ?assertEqual(maps:merge(ExpectedResult1, ExpectedResult2), Result5),

    ?assertNotCalled(kamock_metadata, handle_metadata_request, '_').

brokers() ->
    {ok, _, Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    {ok, _} = kafine_metadata_cache:start_link(?REF),

    Result = kafine_metadata_cache:brokers(?REF),

    ExpectedBrokers = lists:map(
        fun(Broker) -> maps:with([host, port, node_id], Broker) end, Brokers
    ),

    ?assertEqual(ExpectedBrokers, Result).

repeated_brokers_calls_only_fetch_once() ->
    {ok, _, Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    {ok, _} = kafine_metadata_cache:start_link(?REF),

    Result = kafine_metadata_cache:brokers(?REF),

    ExpectedBrokers = lists:map(
        fun(Broker) -> maps:with([host, port, node_id], Broker) end, Brokers
    ),

    ?assertEqual(ExpectedBrokers, Result),

    meck:reset(kamock_metadata),

    Result2 = kafine_metadata_cache:brokers(?REF),
    ?assertEqual(ExpectedBrokers, Result2),

    ?assertNotCalled(kamock_metadata, handle_metadata_request, '_').

brokers_does_not_fetch_if_brokers_exist_from_partitions_call() ->
    {ok, _, Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    {ok, _} = kafine_metadata_cache:start_link(?REF),

    kafine_metadata_cache:partitions(?REF, ?TOPIC_NAME),

    meck:reset(kamock_metadata),

    Result = kafine_metadata_cache:brokers(?REF),

    ExpectedBrokers = lists:map(
        fun(Broker) -> maps:with([host, port, node_id], Broker) end, Brokers
    ),
    ?assertEqual(ExpectedBrokers, Result),

    ?assertNotCalled(kamock_metadata, handle_metadata_request, '_').

refresh_picks_up_updated_metadata() ->
    {ok, Cluster, Brokers = [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    {ok, _} = kafine_metadata_cache:start_link(?REF),

    PartitionsResult = kafine_metadata_cache:partitions(?REF, ?TOPIC_NAME),

    ExpectedPartitions = #{
        ?TOPIC_NAME => #{
            0 => #{leader => 101, replicas => [101, 102], isr => [101, 102]},
            1 => #{leader => 102, replicas => [102, 101], isr => [102, 101]}
        }
    },
    ?assertEqual(ExpectedPartitions, PartitionsResult),

    BrokersResult = kafine_metadata_cache:brokers(?REF),

    ExpectedBrokers = lists:map(
        fun(Broker) -> maps:with([host, port, node_id], Broker) end, Brokers
    ),
    ?assertEqual(ExpectedBrokers, BrokersResult),

    % Add a broker
    {ok, NewBroker} = kamock_broker:start({?CLUSTER_REF, 103}, #{node_id => 103}, Cluster),

    % Change partition metadata
    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        kamock_metadata_response_topic:partitions([0, 1, 2])
    ),

    ok = kafine_metadata_cache:refresh(?REF, [?TOPIC_NAME, ?TOPIC_NAME_2]),

    meck:reset(kamock_metadata),

    % Query partitions again
    PartitionsResult2 = kafine_metadata_cache:partitions(?REF, ?TOPIC_NAME),

    ExpectedPartitions2 = #{
        ?TOPIC_NAME => #{
            0 => #{leader => 101, replicas => [101, 102], isr => [101, 102]},
            1 => #{leader => 102, replicas => [102, 101], isr => [102, 101]},
            2 => #{leader => 101, replicas => [101, 102], isr => [101, 102]}
        }
    },
    ?assertEqual(ExpectedPartitions2, PartitionsResult2),

    % Query brokers again
    BrokersResult2 = kafine_metadata_cache:brokers(?REF),

    ExpectedBrokers2 = ExpectedBrokers ++ [maps:with([host, port, node_id], NewBroker)],
    ?assertEqual(ExpectedBrokers2, BrokersResult2),

    ?assertNotCalled(kamock_metadata, handle_metadata_request, '_').

concurrent_refresh_attempts_only_fetch_once() ->
    {ok, _, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    telemetry_test:attach_event_handlers(self(), [
        [kafine, metadata, refresh, start]
    ]),

    % Add a delay to metadata responses so we can send another refresh while the first is in flgiht
    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        fun(Topic, Env) ->
            timer:sleep(50),
            F = kamock_metadata_response_topic:partitions([0, 1]),
            F(Topic, Env)
        end
    ),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    {ok, _} = kafine_metadata_cache:start_link(?REF),

    async_refresh(?REF, [?TOPIC_NAME, ?TOPIC_NAME_2], self()),
    % wait until the first request is being handled
    ?assertReceived({[kafine, metadata, refresh, start], _, _, _}),
    async_refresh(?REF, [?TOPIC_NAME], self()),

    ?assertReceived(done),
    ?assertReceived(done),

    ?assertEqual([[?TOPIC_NAME, ?TOPIC_NAME_2]], metadata_request_history()).

concurrent_refresh_with_additional_topics_fetches_all_new_topics() ->
    {ok, _, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102]),

    telemetry_test:attach_event_handlers(self(), [
        [kafine, metadata, refresh, start]
    ]),

    % Add a delay to metadata responses so we can send another refresh while the first is in flgiht
    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        fun(Topic, Env) ->
            timer:sleep(50),
            F = kamock_metadata_response_topic:partitions([0, 1]),
            F(Topic, Env)
        end
    ),

    {ok, _} = kafine_bootstrap:start_link(?REF, Bootstrap, ?CONNECTION_OPTIONS),

    {ok, _} = kafine_metadata_cache:start_link(?REF),

    async_refresh(?REF, [?TOPIC_NAME], self()),
    % wait until the first request is being handled
    ?assertReceived({[kafine, metadata, refresh, start], _, _, _}),
    async_refresh(?REF, [?TOPIC_NAME, ?TOPIC_NAME_2], self()),

    ?assertReceived(done),
    ?assertReceived(done),

    ?assertEqual(
        [
            [?TOPIC_NAME],
            [?TOPIC_NAME, ?TOPIC_NAME_2]
        ],
        metadata_request_history()
    ).

async_refresh(Ref, Topics, ReplyTo) ->
    proc_lib:spawn(fun() ->
        ok = kafine_metadata_cache:refresh(Ref, Topics),
        ReplyTo ! done
    end).

metadata_request_history() ->
    History = meck:history(kamock_metadata),
    Fun = fun
        ({_, {_, handle_metadata_request, [#{topics := Topics}, _]}, _}) ->
            TopicNames = lists:map(fun(#{name := Name}) -> Name end, Topics),
            {true, TopicNames};
        (_) ->
            false
    end,
    lists:filtermap(Fun, History).
