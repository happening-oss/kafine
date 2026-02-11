-module(kafine_parallel_subscription_callback_crash_tests).

-include_lib("eunit/include/eunit.hrl").
-include("assert_meck.hrl").

-define(REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONNECTION_OPTIONS, #{client_id => <<"test_client">>}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s_t", [?MODULE]))).
-define(PARTITION, 61).
-define(TOPICS, [?TOPIC_NAME]).
-define(CALLBACK_MOD, test_consumer_callback).
-define(CALLBACK_ARG, dummy_callback_args).
-define(TOPIC_OPTIONS, #{?TOPIC_NAME => #{initial_offset => earliest}}).
-define(HANDLER_OPTS, #{
    callback_mod => ?CALLBACK_MOD,
    callback_arg => ?CALLBACK_ARG,
    skip_empty_fetches => true,
    error_mode => reset
}).

-define(WAIT_TIMEOUT_MS, 2_000).

% -compile(nowarn_unused_function).

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, undefined} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    meck:new(kamock_offset_fetch_response_partition, [passthrough]),

    meck:new(kafine_fetcher),
    meck:expect(kafine_fetcher, whereis, fun(_) -> self() end),
    meck:expect(kafine_fetcher, set_topic_partitions, fun(_, _) -> ok end),
    meck:expect(kafine_fetcher, fetch, fun(_, _, _, _, _, _) -> ok end),

    meck:new(kafine_parallel_handler, [passthrough]),

    ok.

cleanup(_) ->
    meck:unload().

kafine_parallel_subscription_callback_test_() ->
    {foreach, spawn, fun setup/0, fun cleanup/1, [
        fun restarts_exiting_partition_handler/0,
        fun restarts_from_newly_committed_offset/0,
        fun retry_fetch_after_crash/0,
        fun skip_last_fetch_after_crash/0
    ]}.

restarts_exiting_partition_handler() ->
    setup_broker(?REF),
    {ok, Pid} = kafine_parallel_subscription_impl:start_link(
        ?REF,
        kafine_parallel_subscription_callback:validate_options(
            #{
                topic_options => ?TOPIC_OPTIONS,
                callback_mod => ?CALLBACK_MOD,
                callback_arg => ?CALLBACK_ARG
            }
        )
    ),
    {ok, ?REF} = kafine_parallel_subscription_callback:init(?REF),

    % callback should crash on first end batch, after that behave normally
    meck:expect(
        test_consumer_callback,
        end_record_batch,
        5,
        meck:seq([
            meck:raise(exit, crash_it),
            fun(_T, _P, _N, _Info, St) -> {ok, St} end
        ])
    ),

    AssignedPartitions = #{?TOPIC_NAME => [?PARTITION]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),

    ?assertWait(
        kafine_parallel_handler,
        start_link,
        [?REF, {?TOPIC_NAME, ?PARTITION}, earliest, ?HANDLER_OPTS],
        ?WAIT_TIMEOUT_MS
    ),
    meck:reset(kafine_parallel_handler),

    % wait for first fetch
    ?assertWait(kafine_fetcher, fetch, 6, ?WAIT_TIMEOUT_MS),

    % respond with some data, this should result in a crash
    TopicName = ?TOPIC_NAME,
    Offset = 13,
    [{TopicName, ?PARTITION, _, CallbackMod, CallbackArg}] = get_fetch_requests(),
    CallbackMod:handle_partition_data(
        CallbackArg, ?TOPIC_NAME, make_partition_data(?PARTITION, Offset), Offset, not_used
    ),

    % Should restart the handler
    ?assertWait(
        kafine_parallel_handler,
        start_link,
        [?REF, {?TOPIC_NAME, ?PARTITION}, earliest, ?HANDLER_OPTS],
        ?WAIT_TIMEOUT_MS
    ).

restarts_from_newly_committed_offset() ->
    setup_broker(?REF),
    {ok, Pid} = kafine_parallel_subscription_impl:start_link(
        ?REF,
        kafine_parallel_subscription_callback:validate_options(
            #{
                topic_options => ?TOPIC_OPTIONS,
                callback_mod => ?CALLBACK_MOD,
                callback_arg => ?CALLBACK_ARG
            }
        )
    ),
    {ok, ?REF} = kafine_parallel_subscription_callback:init(?REF),

    % callback should crash on first end batch, after that behave normally
    meck:expect(
        test_consumer_callback,
        end_record_batch,
        5,
        meck:seq([
            meck:raise(exit, crash_it),
            fun(_T, _P, _N, _Info, St) -> {ok, St} end
        ])
    ),

    AssignedPartitions = #{?TOPIC_NAME => [?PARTITION]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),

    ?assertWait(
        kafine_parallel_handler,
        start_link,
        [?REF, {?TOPIC_NAME, ?PARTITION}, earliest, ?HANDLER_OPTS],
        ?WAIT_TIMEOUT_MS
    ),
    meck:reset(kafine_parallel_handler),

    % wait for first fetch
    ?assertWait(kafine_fetcher, fetch, 6, ?WAIT_TIMEOUT_MS),

    % commit a new offset
    CommittedOffset = 42,
    setup_offset(CommittedOffset),

    % respond with some data, this should result in a crash
    TopicName = ?TOPIC_NAME,
    [{TopicName, ?PARTITION, Offset, CallbackMod, CallbackArg}] = get_fetch_requests(),
    CallbackMod:handle_partition_data(
        CallbackArg, ?TOPIC_NAME, make_partition_data(?PARTITION, Offset), Offset, not_used
    ),

    % Should restart the handler
    ?assertWait(
        kafine_parallel_handler,
        start_link,
        [?REF, {?TOPIC_NAME, ?PARTITION}, CommittedOffset, ?HANDLER_OPTS],
        ?WAIT_TIMEOUT_MS
    ).

retry_fetch_after_crash() ->
    setup_broker(?REF),
    {ok, Pid} = kafine_parallel_subscription_impl:start_link(
        ?REF,
        kafine_parallel_subscription_callback:validate_options(
            #{
                topic_options => ?TOPIC_OPTIONS,
                callback_mod => ?CALLBACK_MOD,
                callback_arg => ?CALLBACK_ARG,
                error_mode => retry
            }
        )
    ),
    {ok, ?REF} = kafine_parallel_subscription_callback:init(?REF),

    % callback should crash on first end batch, after that behave normally
    meck:expect(
        test_consumer_callback,
        end_record_batch,
        5,
        meck:seq([
            meck:raise(exit, crash_it),
            fun(_T, _P, _N, _Info, St) -> {ok, St} end
        ])
    ),

    AssignedPartitions = #{?TOPIC_NAME => [?PARTITION]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),

    ?assertWait(
        kafine_parallel_handler,
        start_link,
        [?REF, {?TOPIC_NAME, ?PARTITION}, earliest, '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:reset(kafine_parallel_handler),

    % wait for first fetch
    ?assertWait(kafine_fetcher, fetch, 6, ?WAIT_TIMEOUT_MS),

    % respond with some data, this should result in a crash
    TopicName = ?TOPIC_NAME,
    Offset = 13,
    [{TopicName, ?PARTITION, _, CallbackMod, CallbackArg}] = get_fetch_requests(),
    CallbackMod:handle_partition_data(
        CallbackArg, ?TOPIC_NAME, make_partition_data(?PARTITION, Offset), Offset, not_used
    ),

    % Should restart the handler
    ?assertWait(
        kafine_parallel_handler,
        start_link,
        [?REF, {?TOPIC_NAME, ?PARTITION}, Offset, '_'],
        ?WAIT_TIMEOUT_MS
    ).

skip_last_fetch_after_crash() ->
    setup_broker(?REF),
    {ok, Pid} = kafine_parallel_subscription_impl:start_link(
        ?REF,
        kafine_parallel_subscription_callback:validate_options(
            #{
                topic_options => ?TOPIC_OPTIONS,
                callback_mod => ?CALLBACK_MOD,
                callback_arg => ?CALLBACK_ARG,
                error_mode => skip
            }
        )
    ),
    {ok, ?REF} = kafine_parallel_subscription_callback:init(?REF),

    % callback should crash on first end batch, after that behave normally
    meck:expect(
        test_consumer_callback,
        end_record_batch,
        5,
        meck:seq([
            meck:raise(exit, crash_it),
            fun(_T, _P, _N, _Info, St) -> {ok, St} end
        ])
    ),

    AssignedPartitions = #{?TOPIC_NAME => [?PARTITION]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),

    ?assertWait(
        kafine_parallel_handler,
        start_link,
        [?REF, {?TOPIC_NAME, ?PARTITION}, earliest, '_'],
        ?WAIT_TIMEOUT_MS
    ),
    meck:reset(kafine_parallel_handler),

    % wait for first fetch
    ?assertWait(kafine_fetcher, fetch, 6, ?WAIT_TIMEOUT_MS),

    % respond with some data, this should result in a crash
    TopicName = ?TOPIC_NAME,
    Offset = 13,
    [{TopicName, ?PARTITION, _, CallbackMod, CallbackArg}] = get_fetch_requests(),
    CallbackMod:handle_partition_data(
        CallbackArg, ?TOPIC_NAME, make_partition_data(?PARTITION, Offset), Offset, not_used
    ),

    % Should restart the handler
    ?assertWait(
        kafine_parallel_handler,
        start_link,
        [?REF, {?TOPIC_NAME, ?PARTITION}, Offset + 1, '_'],
        ?WAIT_TIMEOUT_MS
    ).

setup_broker(Ref) ->
    setup_broker(Ref, true).

setup_broker(Ref, StartCoordinator) ->
    {ok, Broker} = kamock_broker:start(Ref),
    {ok, _} = kafine_bootstrap:start_link(Ref, Broker, ?CONNECTION_OPTIONS),
    case StartCoordinator of
        true ->
            {ok, _} = kafine_coordinator:start_link(
                Ref, <<"test_group">>, ?TOPICS, ?CONNECTION_OPTIONS, #{}
            );
        false ->
            ok
    end,
    Broker.

get_fetch_requests() ->
    History = meck:history(kafine_fetcher),
    lists:filtermap(
        fun
            (
                {_,
                    {kafine_fetcher, fetch, [_, Topic, Partition, Offset, CallbackMod, CallbackArg]},
                    _}
            ) ->
                {true, {Topic, Partition, Offset, CallbackMod, CallbackArg}};
            (_) ->
                false
        end,
        History
    ).

make_partition_data(Partition, earliest) ->
    make_partition_data(Partition, 0);
make_partition_data(Partition, Offset) ->
    kamock_partition_data:make_single_message(
        Partition,
        Offset,
        Offset,
        Offset + 1,
        #{key => <<"key">>, value => <<"value">>}
    ).

setup_offset(Offset) ->
    meck:expect(
        kamock_offset_fetch_response_partition,
        make_offset_fetch_response_partition,
        fun(_, _, _) ->
            #{
                partition_index => ?PARTITION,
                committed_offset => Offset,
                metadata => <<>>,
                error_code => 0
            }
        end
    ).
