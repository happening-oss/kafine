-module(kafine_consumer_pause_batch_tests).
-include_lib("eunit/include/eunit.hrl").

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
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, dummy} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    meck:new(kamock_offset_fetch_response_partition, [passthrough]),
    ok.

cleanup(_) ->
    meck:unload().

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun end_record_batch_pause/0,
        fun end_record_batch_pause_all/0
    ]}.

end_record_batch_pause() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    set_initial_offsets(10, 12),

    % Pretend that there are some messages.
    MessageBuilder = fun(_T, _P, O) ->
        Key = iolist_to_binary(io_lib:format("key~B", [O])),
        #{key => Key}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 14, 3, MessageBuilder)
    ),

    % Pause one of the partitions.
    meck:expect(test_consumer_callback, end_record_batch, fun
        (_T, _P = ?PARTITION_1, _M, _I, St) -> {pause, St};
        (_T, _P, _M, _I, St) -> {ok, St}
    end),

    TopicName = ?TOPIC_NAME,
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA,
        [
            coordinator(?CONSUMER_REF, [TopicName]),
            parallel_callback(?CONSUMER_REF, ?TOPIC_OPTIONS)
        ]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1, ?PARTITION_2]}, ?CONSUMER_REF
    ),

    % Wait until we've caught up.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, 12, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, 14, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see fetches to both partitions.
    ?assertEqual(
        #{?PARTITION_1 => [10], ?PARTITION_2 => [12]},
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    % Produce some more messages.
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 18, 3, MessageBuilder)
    ),

    % First partition is paused; wait until second partition catches up.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, 18, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see fetches to only one partition.
    % There are two fetches because offset 14 falls into a batch, 15 is the next batch, then 18 is EOF.
    ?assertEqual(
        #{?PARTITION_2 => [14, 15]},
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

end_record_batch_pause_all() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    set_initial_offsets(10, 12),

    % Pretend that there are some messages.
    MessageBuilder = fun(_T, _P, O) ->
        Key = iolist_to_binary(io_lib:format("key~B", [O])),
        #{key => Key}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 14, 3, MessageBuilder)
    ),

    % Pause all of the partitions.
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _M, _I, St) -> {pause, St} end),

    TopicName = ?TOPIC_NAME,
    {ok, Sup} = kafine_consumer_sup:start_link(
        ?CONSUMER_REF,
        Broker,
        ?CONNECTION_OPTIONS,
        ?CONSUMER_OPTIONS,
        [TopicName],
        ?TOPIC_OPTIONS,
        ?FETCHER_METADATA,
        [
            coordinator(?CONSUMER_REF, [TopicName]),
            parallel_callback(?CONSUMER_REF, ?TOPIC_OPTIONS)
        ]
    ),
    kafine_parallel_subscription_callback:subscribe_partitions(
        not_used, #{TopicName => [?PARTITION_1, ?PARTITION_2]}, ?CONSUMER_REF
    ),

    % Wait until we've caught up.
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_1, 12, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        test_consumer_callback,
        end_record_batch,
        ['_', ?PARTITION_2, 14, '_', '_'],
        ?WAIT_TIMEOUT_MS
    ),

    % We should see fetches to both partitions.
    ?assertEqual(
        #{?PARTITION_1 => [10], ?PARTITION_2 => [12]},
        fetch_request_history()
    ),
    meck:reset(kamock_partition_data),

    % Produce some more messages.
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 18, 3, MessageBuilder)
    ),

    % kamock waits 50ms before responding with data. Add 50% to be safe
    timer:sleep(75),

    ?assertEqual(#{}, fetch_request_history()),

    [Pid] = kafine_node_fetcher_sup:list_children(?CONSUMER_REF),
    ?assertMatch(#{state := request_job}, kafine_node_fetcher:info(Pid)),

    kafine_consumer_sup:stop(Sup),
    kamock_broker:stop(Broker),
    ok.

fetch_request_history() ->
    maps:groups_from_list(
        fun({K, _}) -> K end,
        fun({_, V}) -> V end,
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

set_initial_offsets(Part1Offset, Part2Offset) ->
    meck:expect(kamock_offset_fetch_response_partition, make_offset_fetch_response_partition,
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

coordinator(Ref, Topics) ->
    #{
        id => kafine_coordinator,
        start => {kafine_coordinator, start_link, [
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

parallel_callback(Ref, TopicOptions) ->
    Options = kafine_parallel_subscription_callback:validate_options(
        #{
            topic_options => TopicOptions,
            callback_mod => test_consumer_callback,
            callback_arg => ?CALLBACK_ARGS
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
