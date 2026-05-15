-module(kafine_consumer_pause_tests).
-include_lib("eunit/include/eunit.hrl").

-include("assert_meck.hrl").
-include("assert_received.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 1).
-define(PARTITION_2, 2).
-define(CONNECTION_OPTIONS, kafine_connection_options:validate_options(#{})).
-define(CONSUMER_OPTIONS, kafine_consumer_options:validate_options(#{})).
-define(TOPIC_OPTIONS, kafine_topic_options:validate_options([?TOPIC_NAME], #{})).
-define(FETCHER_METADATA, #{}).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun start_paused/0,
        fun pause/0,
        fun pause_all/0
    ]}.

setup() ->
    meck:new(kamock_fetch, [passthrough]),
    meck:new(kamock_partition_data, [passthrough]),
    meck:new(kamock_metadata_response_partition, [passthrough]),

    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, handle_partition_data, fun(_T, _P, _PD, St) ->
        {ok, St}
    end),
    ok.

cleanup(_) ->
    meck:unload().

start_paused() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    set_initial_offsets(10, 12),

    % ConsumerCallback:init is called by kafine_consumer, not kafine_node_consumer, so this doesn't actually get
    % invoked. But maybe it's more readable if we show how you _would_ start paused.
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {pause, ?CALLBACK_STATE} end),

    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, parallel_handler, pause]
    ]),

    TopicName = ?TOPIC_NAME,
    {ok, Sup} = start_consumer(?CONSUMER_REF, Broker, TopicName, ?TOPIC_OPTIONS),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1, ?PARTITION_2]}, ?CONSUMER_REF
    ),

    ?assertReceived(
        {[kafine, parallel_handler, pause], TelemetryRef, _, #{
            topic := TopicName, partition := ?PARTITION_1
        }}
    ),
    ?assertReceived(
        {[kafine, parallel_handler, pause], TelemetryRef, _, #{
            topic := TopicName, partition := ?PARTITION_2
        }}
    ),

    % should be two handlers, both paused
    #{children := Children} = kafine_parallel_subscription_impl:info(?CONSUMER_REF),
    ?assertEqual(2, maps:size(Children)),
    maps:foreach(
        fun(_, Info) ->
            ?assertMatch(#{status := paused}, Info)
        end,
        Children
    ),

    timer:sleep(75),
    ?assertNotCalled(kamock_fetch, handle_fetch_request, '_'),

    stop_consumer(Sup),
    kamock_broker:stop(Broker),
    ok.

pause() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    set_initial_offsets(10, 12),

    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, parallel_handler, pause]
    ]),

    TopicName = ?TOPIC_NAME,

    % Pretend that there are some messages.
    kafine_kamock:produce(0, 14),

    % Pause one of the partitions when it gets to a particular offset.
    meck:expect(test_consumer_callback, handle_partition_data, fun
        (_T, _P = ?PARTITION_1, PartitionData, St) ->
            pause_at_offset(11, PartitionData, St);
        (_T, _P, _M, St) ->
            {ok, St}
    end),

    {ok, Sup} = start_consumer(?CONSUMER_REF, Broker, TopicName, ?TOPIC_OPTIONS),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1, ?PARTITION_2]}, ?CONSUMER_REF
    ),

    ?assertReceived(
        {[kafine, parallel_handler, pause], TelemetryRef, _, #{
            topic := TopicName, partition := ?PARTITION_1
        }}
    ),

    % Wait until we've caught up.
    ?assertWait(
        4,
        test_consumer_callback,
        handle_partition_data,
        '_',
        ?WAIT_TIMEOUT_MS
    ),

    % should be two handlers, one paused
    #{children := Children} = kafine_parallel_subscription_impl:info(?CONSUMER_REF),
    ?assertEqual(2, maps:size(Children)),
    ?assertMatch(
        #{
            ?PARTITION_1 := #{status := paused, next_offset := 12},
            ?PARTITION_2 := #{status := active, next_offset := 14}
        },
        info_by_partition(Children)
    ),

    % We should see fetches to both partitions.
    ?assertEqual(
        #{?PARTITION_1 => [10, 11], ?PARTITION_2 => [12, 13]},
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    % Produce some more messages.
    kafine_kamock:produce(0, 18),

    % First partition is paused; wait until second partition catches up.
    ?assertWait(
        test_consumer_callback,
        handle_partition_data,
        ['_', ?PARTITION_2, meck:is(has_next_offset(18)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see fetches to only one partition.
    ?assertEqual(
        #{?PARTITION_2 => [14, 15, 16, 17]},
        fetch_request_history()
    ),

    stop_consumer(Sup),
    kamock_broker:stop(Broker),
    ok.

pause_all() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    set_initial_offsets(10, 12),

    TopicName = ?TOPIC_NAME,

    % Pretend that there are some messages.
    kafine_kamock:produce(0, 14),

    % Pause both of the partitions when they get to a particular offset.
    meck:expect(test_consumer_callback, handle_partition_data, fun(_T, _P, PartitionData, St) ->
        pause_at_offset(13, PartitionData, St)
    end),

    {ok, Sup} = start_consumer(?CONSUMER_REF, Broker, TopicName, ?TOPIC_OPTIONS),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1, ?PARTITION_2]}, ?CONSUMER_REF
    ),

    % Wait until we've caught up.
    ?assertWait(
        test_consumer_callback,
        handle_partition_data,
        ['_', ?PARTITION_1, meck:is(has_next_offset(14)), '_'],
        ?WAIT_TIMEOUT_MS
    ),
    ?assertWait(
        test_consumer_callback,
        handle_partition_data,
        ['_', ?PARTITION_2, meck:is(has_next_offset(14)), '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see fetches to both partitions.
    ?assertEqual(
        #{?PARTITION_1 => [10, 11, 12, 13], ?PARTITION_2 => [12, 13]},
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),
    meck:reset(kamock_fetch),

    % Produce some more messages.
    kafine_kamock:produce(0, 18),

    % should be two handlers, both paused
    #{children := Children} = kafine_parallel_subscription_impl:info(?CONSUMER_REF),
    ?assertEqual(2, maps:size(Children)),
    maps:foreach(
        fun(_, Info) ->
            ?assertMatch(#{status := paused}, Info)
        end,
        Children
    ),

    timer:sleep(75),
    ?assertNotCalled(kamock_fetch, handle_fetch_request, '_'),

    stop_consumer(Sup),
    kamock_broker:stop(Broker),
    ok.

set_initial_offsets(Part1Offset, Part2Offset) ->
    meck:expect(
        kamock_offset_fetch_response_partition,
        make_offset_fetch_response_partition,
        fun
            (_T, ?PARTITION_1, _) -> offset_fetch_result(?PARTITION_1, Part1Offset);
            (_T, ?PARTITION_2, _) -> offset_fetch_result(?PARTITION_2, Part2Offset)
        end
    ).

offset_fetch_result(PartitionIndex, Offset) ->
    #{
        partition_index => PartitionIndex,
        committed_offset => Offset,
        metadata => <<>>,
        error_code => 0
    }.

start_consumer(Ref, Broker, TopicName, TopicOptions) ->
    kafine_consumer_sup:start_link(
        Ref,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        TopicOptions,
        ?FETCHER_METADATA,
        [
            coordinator(Ref, [TopicName]),
            parallel_callback(Ref, TopicOptions, ?FETCHER_METADATA)
        ]
    ).

stop_consumer(Pid) ->
    kafine_consumer_sup:stop(Pid).

coordinator(Ref, Topics) ->
    #{
        id => kafine_coordinator,
        start =>
            {kafine_coordinator, start_link, [
                Ref,
                atom_to_binary(?MODULE),
                Topics,
                ?CONNECTION_OPTIONS,
                kafine_membership_options:validate_options(#{
                    subscription_callback => {kafine_parallel_subscription_callback, undefined},
                    assignment_callback => {kafine_noop_assignment_callback, undefined}
                })
            ]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafine_coordinator]
    }.

parallel_callback(Ref, TopicOptions, Metadata) ->
    Options = kafine_parallel_subscription_callback:validate_options(
        #{
            topic_options => TopicOptions,
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS
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

fetch_request_history() ->
    TopicOffsets =
        lists:filtermap(
            fun
                ({_, {_, make_partition_data, [_, #{partition := P, fetch_offset := O}, _]}, _}) ->
                    {true, {P, O}};
                (_) ->
                    false
            end,
            meck:history(kamock_partition_data)
        ),
    maps:groups_from_list(fun({P, _}) -> P end, fun({_, O}) -> O end, TopicOffsets).

info_by_partition(Children) ->
    maps:fold(fun(_, Info = #{partition := P}, M) -> M#{P => Info} end, #{}, Children).

pause_at_offset(ExpectedOffset, PartitionData, State) ->
    case
        kafine_partition_data:reduce_while(
            fun
                (#{offset := Offset}, {_, Acc}) when
                    Offset =:= ExpectedOffset
                ->
                    {halt, {paused, Acc}};
                (_, {_, Acc}) ->
                    {cont, {active, Acc}}
            end,
            {active, State},
            PartitionData
        )
    of
        {{active, NextState}, _NextOffset} -> {ok, NextState};
        {{paused, NextState}, NextOffset} -> {pause, NextOffset, NextState}
    end.

has_next_offset(ExpectedNextOffset) ->
    fun(PartitionData) ->
        kafine_partition_data:next_offset(PartitionData) =:= ExpectedNextOffset
    end.
