-module(kafine_consumer_telemetry_tests).
-include_lib("eunit/include/eunit.hrl").

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
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    ok.

cleanup(_) ->
    meck:unload().

single_message_fetch() ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, node_fetcher, fetch]
    ]),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    Message = #{key => <<"key">>, value => <<"value">>, headers => []},
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
        [parallel_callback(?CONSUMER_REF, ?TOPIC_OPTIONS)]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION]}, ?CONSUMER_REF
    ),

    % We should see two record batches, one with a single message, one empty:
    meck:wait(2, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    % Did we see the telemetry we expected?
    Messages = flush(),
    ?assertMatch(
        [
            {[kafine, node_fetcher, fetch], TelemetryRef, #{fetching := #{TopicName := [?PARTITION]}}, #{
                ref := _, cluster_id := <<"ClusterId1">>, node_id := 101
            }}
            | _
        ],
        Messages
    ),

    telemetry:detach(TelemetryRef),

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

parallel_callback(Ref, TopicOptions) ->
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
        start => {kafine_parallel_subscription_impl, start_link, [Ref, Options]},
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
