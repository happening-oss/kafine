-module(kafine_consumer_telemetry_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/api_key.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION, 1).
-define(CONNECTION_OPTIONS, kafine_connection_options:validate_options(#{})).
-define(CONSUMER_OPTIONS, kafine_consumer_options:validate_options(#{})).
-define(TOPIC_OPTIONS, kafine_topic_options:validate_options([?TOPIC_NAME], #{})).
-define(FETCHER_METADATA, #{cluster_id => <<"ClusterId1">>}).
-define(CALLBACK_ARGS, undefined).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun single_message_fetch/0
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, dummy} end),
    meck:expect(test_consumer_callback, handle_partition_data, fun(_T, _P, _PD, St) ->
        {ok, St}
    end),
    ok.

cleanup(_) ->
    meck:unload().

single_message_fetch() ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, node_fetcher, fetch],
        [kafine, connection, request, stop],
        [kafine, parallel_handler, fetch, stop]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    Message = #{
        key => <<"key">>,
        value => <<"value">>,
        headers => [],
        timestamp => erlang:system_time(millisecond)
    },
    meck:expect(kamock_partition_data, make_partition_data, kamock_partition_data:single(Message)),

    TopicName = ?TOPIC_NAME,
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA,
        [parallel_callback(?CONSUMER_REF, ?TOPIC_OPTIONS, ?FETCHER_METADATA)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION]}, ?CONSUMER_REF
    ),

    % We should see two record batches, one with a single message, one empty:
    meck:wait(2, test_consumer_callback, handle_partition_data, '_', ?WAIT_TIMEOUT_MS),

    % Did we see the telemetry we expected?
    Messages = flush(),
    ?assertMatch(
        [
            {
                [kafine, node_fetcher, fetch],
                TelemetryRef,
                #{fetching := #{TopicName := [?PARTITION]}},
                #{ref := _, cluster_id := <<"ClusterId1">>, node_id := 101}
            }
            | _
        ],
        [M || M = {[kafine, node_fetcher, fetch], _, _, _} <- Messages]
    ),

    ?assertMatch(
        [
            {[kafine, connection, request, stop], TelemetryRef, #{response := _}, #{
                api_key := _,
                api_version := _,
                ref := _,
                cluster_id := <<"ClusterId1">>,
                node_id := 101
            }}
        ],
        lists:filter(
            fun
                ({[kafine, connection, request, stop], _, _, #{api_key := ?LIST_OFFSETS}}) -> true;
                (_) -> false
            end,
            Messages
        )
    ),

    ?assertMatch(
        [
            {[kafine, connection, request, stop], TelemetryRef, #{response := _}, #{
                api_key := _,
                api_version := _,
                ref := _,
                cluster_id := <<"ClusterId1">>,
                node_id := 101
            }}
            | _
        ],
        [M || M = {[kafine, connection, request, stop], _, _, #{api_key := ?FETCH}} <- Messages]
    ),

    ?assertMatch(
        [
            {
                [kafine, parallel_handler, fetch, stop],
                TelemetryRef,
                #{
                    monotonic_time := _,
                    system_time := _,
                    duration := _,

                    log_start_offset := _,
                    high_watermark := _,
                    last_stable_offset := _,
                    next_offset := _
                },
                #{
                    ref := _,
                    cluster_id := <<"ClusterId1">>,
                    topic := _,
                    partition := _,
                    known_offset := _
                }
            }
            | _
        ],
        [M || M = {[kafine, parallel_handler, fetch, stop], _, _, _} <- Messages]
    ),

    telemetry:detach(TelemetryRef),

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

parallel_callback(Ref, TopicOptions, Metadata) ->
    Options = kafine_parallel_subscription_callback:validate_options(
        #{
            topic_options => TopicOptions,
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS,
            skip_empty_fetches => false
        }
    ),

    #{
        id => kafine_parallel_subscription,
        start => {kafine_parallel_subscription_impl, start_link, [Ref, Options, Metadata]},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [kafine_parallel_subscription_impl]
    }.

flush() ->
    flush([]).

flush(Acc) ->
    receive
        M ->
            flush([M | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.
