-module(kafine_parallel_subscription_callback_tests).

-include_lib("eunit/include/eunit.hrl").
-include("assert_meck.hrl").

-define(REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONNECTION_OPTIONS, #{client_id => <<"test_client">>}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s_t", [?MODULE]))).
-define(TOPIC_NAME_2, <<(?TOPIC_NAME)/binary, "_2">>).
-define(TOPICS, [?TOPIC_NAME, ?TOPIC_NAME_2]).
-define(CALLBACK_MOD, test_consumer_callback).
-define(CALLBACK_ARG, dummy_callback_args).
-define(TOPIC_OPTIONS, #{
    ?TOPIC_NAME => #{initial_offset => earliest},
    ?TOPIC_NAME_2 => #{initial_offset => -1}
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

    meck:new(kafine_parallel_handler, [stub_all]),
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(_, _, _, _) ->
            {ok, make_link_pid()}
        end
    ),
    meck:expect(kafine_parallel_handler, stop, fun(_, _) -> ok end),
    ok.

cleanup(_) ->
    meck:unload().

kafine_parallel_subscription_callback_test_() ->
    {foreach, spawn, fun setup/0, fun cleanup/1, [
        fun subscribe_partitions_starts_proc_for_each_topic_partition/0,
        fun subscribe_partitions_uses_topic_initial_offset_if_no_committed_offset/0,
        fun subscribe_partitions_uses_topic_initial_offset_if_no_offset_fetcher/0,
        fun restarts_exiting_partition_handler/0,
        fun exits_on_repeated_partition_handler_failure/0,
        fun does_not_exit_if_multiple_different_handlers_fail/0,
        fun uses_initial_offset_on_restart_if_no_committed_offset/0,
        fun uses_initial_offset_on_restart_if_no_offset_fetcher/0,
        fun unsubscribe_all_stops_all_partition_handlers/0,
        fun can_resume_partition/0,
        fun applies_offset_adjustment/0
    ]}.

kafine_parallel_subscription_callback_check_restart_test_() ->
    [
        fun check_restart_adds_new_topic_partition/0,
        fun check_restart_cleans_old_restarts/0,
        fun check_restart_preserves_newish_restarts/0,
        fun check_restart_returns_false_for_too_many_recent_restarts_on_same_topic_partition/0
    ].

subscribe_partitions_starts_proc_for_each_topic_partition() ->
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

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    setup_offsets(#{?TOPIC_NAME => #{0 => 10, 1 => 11}, ?TOPIC_NAME_2 => #{2 => 12}}),
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),

    meck:wait(kafine_fetcher, set_topic_partitions, [?REF, AssignedPartitions], ?WAIT_TIMEOUT_MS),

    assert_handler_started(?REF, ?TOPIC_NAME, 0, 10),
    assert_handler_started(?REF, ?TOPIC_NAME, 1, 11),
    assert_handler_started(?REF, ?TOPIC_NAME_2, 2, 12).

subscribe_partitions_uses_topic_initial_offset_if_no_committed_offset() ->
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

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    setup_offsets(#{?TOPIC_NAME => #{0 => 10, 1 => -1}, ?TOPIC_NAME_2 => #{2 => -1}}),
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),

    meck:wait(kafine_fetcher, set_topic_partitions, [?REF, AssignedPartitions], ?WAIT_TIMEOUT_MS),

    assert_handler_started(?REF, ?TOPIC_NAME, 0, 10),
    assert_handler_started(?REF, ?TOPIC_NAME, 1, earliest),
    assert_handler_started(?REF, ?TOPIC_NAME_2, 2, -1).

subscribe_partitions_uses_topic_initial_offset_if_no_offset_fetcher() ->
    setup_broker(?REF, false),
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

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),

    meck:wait(kafine_fetcher, set_topic_partitions, [?REF, AssignedPartitions], ?WAIT_TIMEOUT_MS),

    assert_handler_started(?REF, ?TOPIC_NAME, 0, earliest),
    assert_handler_started(?REF, ?TOPIC_NAME, 1, earliest),
    assert_handler_started(?REF, ?TOPIC_NAME_2, 2, -1).

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

    HandlerPids = #{
        ?TOPIC_NAME => #{0 => make_pid(), 1 => make_pid()}, ?TOPIC_NAME_2 => #{2 => make_pid()}
    },
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(?REF, {Topic, Partition}, _, _) ->
            HandlerPid = kafine_topic_partition_data:get(Topic, Partition, HandlerPids),
            link(HandlerPid),
            {ok, HandlerPid}
        end
    ),

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),
    meck:wait(3, kafine_parallel_handler, start_link, 4, ?WAIT_TIMEOUT_MS),

    % future starts need a fresh pid
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(_, _, _, _) -> {ok, make_link_pid()} end
    ),

    % We need an offset to re-start at, ensure it's different
    setup_offsets(#{?TOPIC_NAME => #{1 => 42}}),

    meck:reset(kafine_parallel_handler),
    exit(kafine_topic_partition_data:get(?TOPIC_NAME, 1, HandlerPids), simulated_error),

    assert_handler_started(?REF, ?TOPIC_NAME, 1, 42).

exits_on_repeated_partition_handler_failure() ->
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

    HandlerPids = #{
        ?TOPIC_NAME => #{0 => make_pid(), 1 => make_pid()}, ?TOPIC_NAME_2 => #{2 => make_pid()}
    },
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(?REF, {Topic, Partition}, _, _) ->
            HandlerPid = kafine_topic_partition_data:get(Topic, Partition, HandlerPids),
            link(HandlerPid),
            {ok, HandlerPid}
        end
    ),

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),
    meck:wait(3, kafine_parallel_handler, start_link, 4, ?WAIT_TIMEOUT_MS),

    % future starts need a fresh pid
    NewPid = make_link_pid(),
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(_, _, _, _) ->
            link(NewPid),
            {ok, NewPid}
        end
    ),

    meck:reset(kafine_parallel_handler),
    exit(kafine_topic_partition_data:get(?TOPIC_NAME, 1, HandlerPids), simulated_error),

    assert_handler_started(?REF, ?TOPIC_NAME, 1, earliest),

    process_flag(trap_exit, true),
    meck:reset(kafine_parallel_handler),
    exit(NewPid, simulated_error),

    receive
        {'EXIT', P, shutdown} when P =:= Pid -> ok
    after ?WAIT_TIMEOUT_MS ->
        error(timeout)
    end.

does_not_exit_if_multiple_different_handlers_fail() ->
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

    HandlerPids = #{
        ?TOPIC_NAME => #{0 => make_pid(), 1 => make_pid()}, ?TOPIC_NAME_2 => #{2 => make_pid()}
    },
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(?REF, {Topic, Partition}, _, _) ->
            HandlerPid = kafine_topic_partition_data:get(Topic, Partition, HandlerPids),
            link(HandlerPid),
            {ok, HandlerPid}
        end
    ),

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),
    meck:wait(3, kafine_parallel_handler, start_link, 4, ?WAIT_TIMEOUT_MS),

    % future starts need a fresh pid
    NewHandlerPids = #{
        ?TOPIC_NAME => #{0 => make_pid(), 1 => make_pid()}, ?TOPIC_NAME_2 => #{2 => make_pid()}
    },
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(?REF, {Topic, Partition}, _, _) ->
            HandlerPid = kafine_topic_partition_data:get(Topic, Partition, NewHandlerPids),
            link(HandlerPid),
            {ok, HandlerPid}
        end
    ),

    meck:reset(kafine_parallel_handler),
    exit(kafine_topic_partition_data:get(?TOPIC_NAME, 0, HandlerPids), simulated_error),
    exit(kafine_topic_partition_data:get(?TOPIC_NAME, 1, HandlerPids), simulated_error),
    exit(kafine_topic_partition_data:get(?TOPIC_NAME_2, 2, HandlerPids), simulated_error),

    assert_handler_started(?REF, ?TOPIC_NAME, 0, earliest),
    assert_handler_started(?REF, ?TOPIC_NAME, 1, earliest),
    assert_handler_started(?REF, ?TOPIC_NAME_2, 2, -1).

uses_initial_offset_on_restart_if_no_committed_offset() ->
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

    HandlerPids = #{
        ?TOPIC_NAME => #{0 => make_pid(), 1 => make_pid()}, ?TOPIC_NAME_2 => #{2 => make_pid()}
    },
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(?REF, {Topic, Partition}, _, _) ->
            HandlerPid = kafine_topic_partition_data:get(Topic, Partition, HandlerPids),
            link(HandlerPid),
            {ok, HandlerPid}
        end
    ),

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),
    meck:wait(3, kafine_parallel_handler, start_link, 4, ?WAIT_TIMEOUT_MS),

    % future starts need a fresh pid
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(_, _, _, _) -> {ok, make_link_pid()} end
    ),

    % We need an offset to re-start at, ensure it's different
    setup_offsets(#{?TOPIC_NAME => #{1 => -1}}),

    meck:reset(kafine_parallel_handler),
    exit(kafine_topic_partition_data:get(?TOPIC_NAME, 1, HandlerPids), simulated_error),

    assert_handler_started(?REF, ?TOPIC_NAME, 1, earliest).

uses_initial_offset_on_restart_if_no_offset_fetcher() ->
    setup_broker(?REF, false),
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

    HandlerPids = #{
        ?TOPIC_NAME => #{0 => make_pid(), 1 => make_pid()}, ?TOPIC_NAME_2 => #{2 => make_pid()}
    },
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(?REF, {Topic, Partition}, _, _) ->
            HandlerPid = kafine_topic_partition_data:get(Topic, Partition, HandlerPids),
            link(HandlerPid),
            {ok, HandlerPid}
        end
    ),

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),
    meck:wait(3, kafine_parallel_handler, start_link, 4, ?WAIT_TIMEOUT_MS),

    % future starts need a fresh pid
    meck:expect(
        kafine_parallel_handler,
        start_link,
        fun(_, _, _, _) -> {ok, make_link_pid()} end
    ),

    meck:reset(kafine_parallel_handler),
    exit(kafine_topic_partition_data:get(?TOPIC_NAME, 1, HandlerPids), simulated_error),

    assert_handler_started(?REF, ?TOPIC_NAME, 1, earliest).

unsubscribe_all_stops_all_partition_handlers() ->
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

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),
    meck:wait(3, kafine_parallel_handler, start_link, 4, ?WAIT_TIMEOUT_MS),

    {ok, Pid} = kafine_parallel_subscription_callback:unsubscribe_partitions(Pid),

    meck:wait(3, kafine_parallel_handler, stop, 2, ?WAIT_TIMEOUT_MS).

can_resume_partition() ->
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

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),

    meck:wait(kafine_fetcher, set_topic_partitions, [?REF, AssignedPartitions], ?WAIT_TIMEOUT_MS),

    kafine_consumer:resume(?REF, ?TOPIC_NAME, 1, 11),
    ?assertCalled(kafine_parallel_handler, resume, [?REF, ?TOPIC_NAME, 1, 11]).

applies_offset_adjustment() ->
    setup_broker(?REF),

    meck:new(test_offset_callback, [non_strict]),
    meck:expect(test_offset_callback, adjust_committed_offset, fun(Offset) ->
        Offset + 5
    end),

    {ok, Pid} = kafine_parallel_subscription_impl:start_link(
        ?REF,
        kafine_parallel_subscription_callback:validate_options(
            #{
                topic_options => ?TOPIC_OPTIONS,
                callback_mod => ?CALLBACK_MOD,
                callback_arg => ?CALLBACK_ARG,
                offset_callback => test_offset_callback
            }
        )
    ),
    {ok, ?REF} = kafine_parallel_subscription_callback:init(?REF),

    AssignedPartitions = #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]},
    setup_offsets(#{?TOPIC_NAME => #{0 => 10, 1 => 11}, ?TOPIC_NAME_2 => #{2 => 12}}),
    {ok, Pid} = kafine_parallel_subscription_callback:subscribe_partitions(
        unused, AssignedPartitions, Pid
    ),

    meck:wait(kafine_fetcher, set_topic_partitions, [?REF, AssignedPartitions], ?WAIT_TIMEOUT_MS),

    assert_handler_started(?REF, ?TOPIC_NAME, 0, 15),
    assert_handler_started(?REF, ?TOPIC_NAME, 1, 16),
    assert_handler_started(?REF, ?TOPIC_NAME_2, 2, 17).

assert_handler_started(Ref, Topic, Partition, Offset) ->
    ?assertWait(
        kafine_parallel_handler,
        start_link,
        [Ref, {Topic, Partition}, Offset, '_'],
        ?WAIT_TIMEOUT_MS
    ).

% Make a disposable pid we can assert on
make_pid() ->
    proc_lib:spawn(fun() -> receive
        after infinity -> ok
        end end).

make_link_pid() ->
    Pid = make_pid(),
    link(Pid),
    Pid.

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

setup_offsets(Offsets) ->
    meck:expect(
        kamock_offset_fetch_response_partition,
        make_offset_fetch_response_partition,
        fun(Topic, Partition, _) ->
            #{
                partition_index => Partition,
                committed_offset => kafine_maps:get([Topic, Partition], Offsets, -1),
                metadata => <<>>,
                error_code => 0
            }
        end
    ).

check_restart_adds_new_topic_partition() ->
    TopicPartition = {?TOPIC_NAME, 0},
    Result = kafine_parallel_subscription_impl:check_restart(TopicPartition, []),
    ?assertMatch({true, [{TopicPartition, _}]}, Result).

check_restart_cleans_old_restarts() ->
    Now = erlang:monotonic_time(millisecond),
    TopicPartition = {?TOPIC_NAME, 0},
    TopicPartition2 = {?TOPIC_NAME, 1},
    TopicPartition3 = {?TOPIC_NAME_2, 2},
    Restarts = [{TopicPartition, Now - 5_010}, {TopicPartition2, Now - 5_015}, {TopicPartition3, Now - 5_020}],
    Result = kafine_parallel_subscription_impl:check_restart(TopicPartition, Restarts),
    {true, [{TopicPartition, NewRestartTime}]} = Result,
    ?assert(NewRestartTime > Now - 10 andalso NewRestartTime =< Now).

check_restart_preserves_newish_restarts() ->
    Now = erlang:monotonic_time(millisecond),
    TopicPartition = {?TOPIC_NAME, 0},
    TopicPartition2 = {?TOPIC_NAME, 1},
    TopicPartition3 = {?TOPIC_NAME_2, 2},
    Restart2 = {TopicPartition2, Now - 3_015},
    Restart3 = {TopicPartition3, Now - 3_020},
    Restarts = [Restart2, Restart3],
    Result = kafine_parallel_subscription_impl:check_restart(TopicPartition, Restarts),
    ?assertMatch({true, [{TopicPartition, _}, Restart2, Restart3]}, Result).

check_restart_returns_false_for_too_many_recent_restarts_on_same_topic_partition() ->
    Now = erlang:monotonic_time(millisecond),
    TopicPartition = {?TOPIC_NAME, 0},
    TopicPartition2 = {?TOPIC_NAME, 1},
    TopicPartition3 = {?TOPIC_NAME_2, 2},
    Restart1 = {TopicPartition, Now - 3_010},
    Restart2 = {TopicPartition2, Now - 3_015},
    Restart3 = {TopicPartition3, Now - 3_020},
    Restarts = [Restart1, Restart2, Restart3],
    Result = kafine_parallel_subscription_impl:check_restart(TopicPartition, Restarts),
    ?assertMatch({false, [{TopicPartition, _}, Restart1, Restart2, Restart3]}, Result).
