-module(kafine_node_fetcher_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-include("assert_meck.hrl").
-include("assert_received.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(REF, {?MODULE, ?FUNCTION_NAME}).
-define(JOB_ID, 4).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_t_2", [?MODULE, ?FUNCTION_NAME]))).
-define(CONSUMER_OPTIONS, kafine_consumer_options:validate_options(#{})).
-define(TOPIC_OPTIONS, #{
    ?TOPIC_NAME => #{offset_reset_policy => earliest},
    ?TOPIC_NAME_2 => #{offset_reset_policy => -1}
}).
-define(CONNECTION_OPTIONS, #{
    backoff => kafine_backoff:fixed(50)
}).
-define(FETCHER_METADATA, #{}).
-define(WAIT_TIMEOUT_MS, 2_000).

% -compile(nowarn_unused_function).

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, handle_partition_data, fun(_A, _T, _PD, _FO, _S) -> ok end),

    meck:new(kamock_list_offsets, [passthrough]),
    meck:new(kamock_fetch, [passthrough]),

    meck:new(kafine_fetcher, [stub_all]),

    meck:new(kafine_backoff, [passthrough]),

    ok.

cleanup(_) ->
    meck:unload().

kafine_node_consumer_test_() ->
    {foreach, spawn, fun setup/0, fun cleanup/1, [
        fun requests_job_after_start/0,
        fun handles_list_offsets_job/0,
        fun list_offsets_with_replay_returns_zero_if_replay_would_yield_negative_offset/0,
        fun handles_fetch_job/0,
        fun handles_fetch_batch/0,
        fun sends_update_offsets_for_offset_out_of_range_error/0,
        fun sends_give_away_for_not_leader_or_follower_error/0,
        fun sends_repeat_if_callback_requests_repeat/0,
        fun reconnects_and_requests_job_on_disconnect/0,
        fun applies_backoff_on_failed_reconnect/0
    ]}.

requests_job_after_start() ->
    {ok, Broker = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF),

    {ok, Fetcher} = kafine_node_fetcher:start_link(
        ?REF, ?CONNECTION_OPTIONS, ?CONSUMER_OPTIONS, ?TOPIC_OPTIONS, self(), Broker, ?FETCHER_METADATA
    ),

    assert_job_requested(NodeId, Fetcher),

    kafine_node_fetcher:stop(Fetcher),
    kamock_broker:stop(Broker).

handles_list_offsets_job() ->
    {ok, Broker = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF),

    {ok, Fetcher} = kafine_node_fetcher:start_link(
        ?REF, ?CONNECTION_OPTIONS, ?CONSUMER_OPTIONS, ?TOPIC_OPTIONS, self(), Broker, ?FETCHER_METADATA
    ),
    assert_job_requested(NodeId, Fetcher),

    meck:expect(kamock_list_offsets_partition_response, make_list_offsets_partition_response, [
        offsets(?TOPIC_NAME, 0, 12, 23),
        offsets(?TOPIC_NAME, 1, 34, 45),
        offsets(?TOPIC_NAME_2, 2, 56, 67)
    ]),

    kafine_node_fetcher:job(
        Fetcher,
        {?JOB_ID, list_offsets, #{
            ?TOPIC_NAME => #{0 => earliest, 1 => latest},
            ?TOPIC_NAME_2 => #{2 => -1}
        }}
    ),

    assert_offsets_updated(NodeId, #{
        ?TOPIC_NAME => #{0 => 12, 1 => 45}, ?TOPIC_NAME_2 => #{2 => 66}
    }),

    kafine_node_fetcher:stop(Fetcher),
    kamock_broker:stop(Broker).

list_offsets_with_replay_returns_zero_if_replay_would_yield_negative_offset() ->
    {ok, Broker = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF),

    {ok, Fetcher} = kafine_node_fetcher:start_link(
        ?REF, ?CONNECTION_OPTIONS, ?CONSUMER_OPTIONS, ?TOPIC_OPTIONS, self(), Broker, ?FETCHER_METADATA
    ),
    assert_job_requested(NodeId, Fetcher),

    meck:expect(kamock_list_offsets_partition_response, make_list_offsets_partition_response, [
        offsets(?TOPIC_NAME, 0, 10, 12),
        offsets(?TOPIC_NAME, 1, 0, 0),
        offsets(?TOPIC_NAME_2, 2, 0, 67)
    ]),

    kafine_node_fetcher:job(
        Fetcher,
        {?JOB_ID, list_offsets, #{
            ?TOPIC_NAME => #{0 => earliest, 1 => -1},
            ?TOPIC_NAME_2 => #{2 => -2}
        }}
    ),

    assert_offsets_updated(NodeId, #{?TOPIC_NAME => #{0 => 10, 1 => 0}, ?TOPIC_NAME_2 => #{2 => 65}}),

    kafine_node_fetcher:stop(Fetcher),
    kamock_broker:stop(Broker).

handles_fetch_job() ->
    {ok, Broker = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF),

    {ok, Fetcher} = kafine_node_fetcher:start_link(
        ?REF, ?CONNECTION_OPTIONS, ?CONSUMER_OPTIONS, ?TOPIC_OPTIONS, self(), Broker, ?FETCHER_METADATA
    ),
    assert_job_requested(NodeId, Fetcher),

    meck:expect(kamock_partition_data, make_partition_data, [
        record(?TOPIC_NAME, 0, 12),
        record(?TOPIC_NAME, 1, 34),
        record(?TOPIC_NAME_2, 2, 56)
    ]),

    kafine_node_fetcher:job(
        Fetcher,
        {?JOB_ID, fetch, #{
            ?TOPIC_NAME => #{
                0 => {12, test_consumer_callback, t1p0},
                1 => {34, test_consumer_callback, t1p1}
            },
            ?TOPIC_NAME_2 => #{
                2 => {56, test_consumer_callback, t2p2}
            }
        }}
    ),

    assert_got_record(?TOPIC_NAME, 0, 12, t1p0),
    assert_got_record(?TOPIC_NAME, 1, 34, t1p1),
    assert_got_record(?TOPIC_NAME_2, 2, 56, t2p2),
    assert_got_completed(NodeId, #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]}),

    kafine_node_fetcher:stop(Fetcher),
    kamock_broker:stop(Broker).

handles_fetch_batch() ->
    {ok, Broker = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF),

    {ok, Fetcher} = kafine_node_fetcher:start_link(
        ?REF, ?CONNECTION_OPTIONS, ?CONSUMER_OPTIONS, ?TOPIC_OPTIONS, self(), Broker, ?FETCHER_METADATA
    ),
    assert_job_requested(NodeId, Fetcher),

    meck:expect(kamock_partition_data, make_partition_data, [
        batch(?TOPIC_NAME, 0, 12, 3),
        batch(?TOPIC_NAME, 1, 32, 4),
        batch(?TOPIC_NAME_2, 2, 55, 5)
    ]),

    kafine_node_fetcher:job(
        Fetcher,
        {?JOB_ID, fetch, #{
            ?TOPIC_NAME => #{
                0 => {12, test_consumer_callback, t1p0},
                1 => {34, test_consumer_callback, t1p1}
            },
            ?TOPIC_NAME_2 => #{
                2 => {56, test_consumer_callback, t2p2}
            }
        }}
    ),

    assert_got_records(?TOPIC_NAME, 0, 12, 14, t1p0),
    assert_got_records(?TOPIC_NAME, 1, 32, 35, t1p1),
    assert_got_records(?TOPIC_NAME_2, 2, 55, 59, t2p2),
    assert_got_completed(NodeId, #{?TOPIC_NAME => [0, 1], ?TOPIC_NAME_2 => [2]}),

    kafine_node_fetcher:stop(Fetcher),
    kamock_broker:stop(Broker).

sends_update_offsets_for_offset_out_of_range_error() ->
    {ok, Broker = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF),

    {ok, Fetcher} = kafine_node_fetcher:start_link(
        ?REF, ?CONNECTION_OPTIONS, ?CONSUMER_OPTIONS, ?TOPIC_OPTIONS, self(), Broker, ?FETCHER_METADATA
    ),
    assert_job_requested(NodeId, Fetcher),

    meck:expect(kamock_partition_data, make_partition_data, [
        kafka_error(?TOPIC_NAME, 0, ?OFFSET_OUT_OF_RANGE),
        record(?TOPIC_NAME, 1, 34),
        kafka_error(?TOPIC_NAME_2, 2, ?OFFSET_OUT_OF_RANGE)
    ]),

    kafine_node_fetcher:job(
        Fetcher,
        {?JOB_ID, fetch, #{
            ?TOPIC_NAME => #{
                0 => {12, test_consumer_callback, t1p0},
                1 => {34, test_consumer_callback, t1p1}
            },
            ?TOPIC_NAME_2 => #{
                2 => {56, test_consumer_callback, t2p2}
            }
        }}
    ),

    assert_got_record(?TOPIC_NAME, 1, 34, t1p1),
    assert_got_job_completion(NodeId, #{
        ?TOPIC_NAME => #{0 => {update_offset, earliest}, 1 => completed},
        ?TOPIC_NAME_2 => #{2 => {update_offset, -1}}
    }),

    kafine_node_fetcher:stop(Fetcher),
    kamock_broker:stop(Broker).

sends_give_away_for_not_leader_or_follower_error() ->
    {ok, Broker = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF),

    {ok, Fetcher} = kafine_node_fetcher:start_link(
        ?REF, ?CONNECTION_OPTIONS, ?CONSUMER_OPTIONS, ?TOPIC_OPTIONS, self(), Broker, ?FETCHER_METADATA
    ),
    assert_job_requested(NodeId, Fetcher),

    meck:expect(kamock_partition_data, make_partition_data, [
        kafka_error(?TOPIC_NAME, 0, ?NOT_LEADER_OR_FOLLOWER),
        record(?TOPIC_NAME, 1, 34),
        kafka_error(?TOPIC_NAME_2, 2, ?NOT_LEADER_OR_FOLLOWER)
    ]),

    kafine_node_fetcher:job(
        Fetcher,
        {?JOB_ID, fetch, #{
            ?TOPIC_NAME => #{
                0 => {12, test_consumer_callback, t1p0},
                1 => {34, test_consumer_callback, t1p1}
            },
            ?TOPIC_NAME_2 => #{
                2 => {56, test_consumer_callback, t2p2}
            }
        }}
    ),

    assert_got_record(?TOPIC_NAME, 1, 34, t1p1),
    assert_got_job_completion(NodeId, #{
        ?TOPIC_NAME => #{0 => give_away, 1 => completed},
        ?TOPIC_NAME_2 => #{2 => give_away}
    }),

    kafine_node_fetcher:stop(Fetcher),
    kamock_broker:stop(Broker).

sends_repeat_if_callback_requests_repeat() ->
    {ok, Broker = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF),

    {ok, Fetcher} = kafine_node_fetcher:start_link(
        ?REF, ?CONNECTION_OPTIONS, ?CONSUMER_OPTIONS, ?TOPIC_OPTIONS, self(), Broker, ?FETCHER_METADATA
    ),
    assert_job_requested(NodeId, Fetcher),

    meck:expect(kamock_partition_data, make_partition_data, [
        record(?TOPIC_NAME, 0, 12),
        record(?TOPIC_NAME, 1, 34),
        record(?TOPIC_NAME_2, 2, 56)
    ]),

    meck:expect(
        test_consumer_callback,
        handle_partition_data,
        fun
            % TOPIC_NAME/0 still returns ok
            (_A, _T, #{partition_index := 0}, _FO, _S) -> ok;
            % Other partitions ask for repeat
            (_A, _T, _PD, _FO, _S) -> repeat
        end
    ),

    kafine_node_fetcher:job(
        Fetcher,
        {?JOB_ID, fetch, #{
            ?TOPIC_NAME => #{
                0 => {12, test_consumer_callback, t1p0},
                1 => {34, test_consumer_callback, t1p1}
            },
            ?TOPIC_NAME_2 => #{
                2 => {56, test_consumer_callback, t2p2}
            }
        }}
    ),

    assert_got_record(?TOPIC_NAME, 0, 12, t1p0),
    assert_got_record(?TOPIC_NAME, 1, 34, t1p1),
    assert_got_record(?TOPIC_NAME_2, 2, 56, t2p2),
    assert_got_job_completion(NodeId, #{
        ?TOPIC_NAME => #{0 => completed, 1 => repeat},
        ?TOPIC_NAME_2 => #{2 => repeat}
    }),

    kafine_node_fetcher:stop(Fetcher),
    kamock_broker:stop(Broker).

reconnects_and_requests_job_on_disconnect() ->
    {ok, Broker = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF),

    telemetry_test:attach_event_handlers(self(), [
        [kafine, node_fetcher, connected],
        [kafine, node_fetcher, disconnected],
        [kamock, protocol, connected]
    ]),

    {ok, Fetcher} = kafine_node_fetcher:start_link(
        ?REF, ?CONNECTION_OPTIONS, ?CONSUMER_OPTIONS, ?TOPIC_OPTIONS, self(), Broker, ?FETCHER_METADATA
    ),

    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, node_fetcher, connected], _, _, _}),

    assert_job_requested(NodeId, Fetcher),
    meck:reset(kafine_fetcher),

    % disconnect the node fetcher
    kamock_broker:drop(Broker),

    ?assertReceived({[kafine, node_fetcher, disconnected], _, _, _}),
    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, node_fetcher, connected], _, _, _}),

    assert_job_requested(NodeId, Fetcher),
    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker)),

    ?assertNotCalled(kafine_backoff, backoff, '_'),

    kafine_node_fetcher:stop(Fetcher),
    kamock_broker:stop(Broker).

applies_backoff_on_failed_reconnect() ->
    {ok, Broker = #{node_id := NodeId, port := Port}} = kamock_broker:start(?BROKER_REF),

    telemetry_test:attach_event_handlers(self(), [
        [kafine, node_fetcher, connected],
        [kafine, node_fetcher, disconnected],
        [kafine, node_fetcher, backoff],
        [kamock, protocol, connected]
    ]),

    meck:expect(kafine_backoff, init, fun(_) -> {backoff_state, 1} end),
    meck:expect(kafine_backoff, reset, fun(State) -> State end),

    {ok, Fetcher} = kafine_node_fetcher:start_link(
        ?REF, ?CONNECTION_OPTIONS, ?CONSUMER_OPTIONS, ?TOPIC_OPTIONS, self(), Broker, ?FETCHER_METADATA
    ),

    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, node_fetcher, connected], _, _, _}),

    assert_job_requested(NodeId, Fetcher),
    meck:reset(kafine_fetcher),

    meck:expect(kafine_backoff, backoff, [
        {[{backoff_state, 1}], {50, {backoff_state, 2}}},
        {[{backoff_state, '_'}], {50, {backoff_state, 3}}}
    ]),

    % Stop the broker, dropping connections and failing future attempts
    kamock_broker:stop(Broker),

    ?assertReceived({[kafine, node_fetcher, disconnected], _, _, _}),

    % should repeatedly back off
    ?assertReceived({[kafine, node_fetcher, backoff], _, _, _}),
    ?assertCalled(kafine_backoff, backoff, [{backoff_state, 1}]),

    ?assertReceived({[kafine, node_fetcher, backoff], _, _, _}),
    ?assertCalled(kafine_backoff, backoff, [{backoff_state, 2}]),

    % Bring broker back up
    {ok, Broker2 = #{node_id := NodeId}} = kamock_broker:start(?BROKER_REF, #{port => Port}),

    % should connect and reset backoff state
    ?assertReceived({[kamock, protocol, connected], _, _, _}),
    ?assertReceived({[kafine, node_fetcher, connected], _, _, _}),
    ?assertCalled(kafine_backoff, reset, [{backoff_state, 3}]),

    assert_job_requested(NodeId, Fetcher),
    ?assertMatch(#{active_connections := 1}, kamock_broker:info(Broker2)),

    kafine_node_fetcher:stop(Fetcher),
    kamock_broker:stop(Broker2).

assert_job_requested(NodeId, Fetcher) ->
    meck:wait(kafine_fetcher, request_job, [self(), NodeId, Fetcher], ?WAIT_TIMEOUT_MS).

offsets(Topic, Partition, Earliest, Latest) ->
    {
        [Topic, is_partition(Partition), '_'],
        kamock_list_offsets_partition_response:range(Earliest, Latest)
    }.

record(Topic, Partition, Offset) ->
    {
        [Topic, is_partition(Partition), '_'],
        kamock_partition_data:range(Offset, Offset + 1, fun build_record/3)
    }.

batch(Topic, Partition, StartOffset, Size) ->
    {
        [Topic, is_partition(Partition), '_'],
        kamock_partition_data:batches(StartOffset, StartOffset + Size + 1, Size, fun build_record/3)
    }.

kafka_error(Topic, Partition, ErrorCode) ->
    {
        [Topic, is_partition(Partition), '_'],
        fun(_, _, _) -> kamock_partition_data:make_error(Partition, ErrorCode) end
    }.

build_record(Topic, Partition, Offset) ->
    Key = iolist_to_binary(io_lib:format("key-~s-~B-~B", [Topic, Partition, Offset])),
    Value = iolist_to_binary(io_lib:format("value-~s-~B-~B", [Topic, Partition, Offset])),
    #{key => Key, value => Value}.

is_partition(Partition) ->
    IsPartition = fun(P) ->
        fun
            (_FetchPartition = #{partition_index := PartitionIndex}) -> PartitionIndex == P;
            (_ListOffsetsPartition = #{partition := PartitionIndex}) -> PartitionIndex == P;
            (_) -> false
        end
    end,
    meck:is(IsPartition(Partition)).

assert_got_record(Topic, Partition, Offset, CallbackArgs) ->
    Expected = build_record(Topic, Partition, Offset),
    meck:wait(
        test_consumer_callback,
        handle_partition_data,
        [
            CallbackArgs,
            Topic,
            meck:is(fun(#{partition_index := P, records := Records}) ->
                P =:= Partition andalso
                    lists:any(
                        fun(Batch) ->
                            batch_contains_record(Offset, Expected, Batch)
                        end,
                        Records
                    )
            end),
            '_',
            '_'
        ],
        ?WAIT_TIMEOUT_MS
    ).

batch_contains_record(Offset, ExpectedRecord, #{base_offset := B, records := Records}) ->
    lists:any(
        fun(R = #{offset_delta := D}) ->
            Offset =:= B + D andalso
                maps:intersect(R, ExpectedRecord) == ExpectedRecord
        end,
        Records
    ).

assert_got_records(Topic, Partition, FirstOffset, LastOffset, CallbackArgs) ->
    lists:foreach(
        fun(Offset) ->
            assert_got_record(Topic, Partition, Offset, CallbackArgs)
        end,
        lists:seq(FirstOffset, LastOffset)
    ).

assert_offsets_updated(NodeId, ExpectedOffsets) ->
    ExpectedResult = kafine_topic_partition_data:map(
        fun(_, _, Offset) -> {update_offset, Offset} end,
        ExpectedOffsets
    ),
    assert_got_job_completion(NodeId, ExpectedResult).

assert_got_completed(NodeId, TopicPartitions) ->
    ExpectedResult = kafine_topic_partitions:map(fun(_, _) -> completed end, TopicPartitions),
    assert_got_job_completion(NodeId, ExpectedResult).

assert_got_job_completion(NodeId, ExpectedResult) ->
    meck:wait(
        kafine_fetcher,
        complete_job,
        [self(), ?JOB_ID, NodeId, meck:is(fun(R) -> R =:= ExpectedResult end)],
        ?WAIT_TIMEOUT_MS
    ).
