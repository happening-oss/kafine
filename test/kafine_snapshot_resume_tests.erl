-module(kafine_snapshot_resume_tests).
-include_lib("eunit/include/eunit.hrl").
-include("assert_meck.hrl").

-define(CLUSTER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(FETCHER_METADATA, #{}).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, {state, ?MODULE}).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {timeout, 30, fun snapshot_parity_resumes_state/0}
    ]}.

setup() ->
    {ok, _} = application:ensure_all_started(kafine),
    ok.

cleanup(_) ->
    meck:unload(),
    application:stop(kafine),
    ok.

%% When a 'snapshot' partition reaches parity, resume the corresponding 'state' partition.
snapshot_parity_resumes_state() ->
    % The original bug was the the kafine_node_consumer deadlocks:
    % 1. It calls the consumer callback process.
    % 2. The consumer callback process calls kafine_node_consumer:resume(), which is in the call, so we deadlock.
    %
    % To reproduce this, partition 0 needs to be on the same node for all topics. kamock, by default, does this.
    {ok, Cluster, [Bootstrap | _]} = kamock_cluster:start(?CLUSTER_REF, [101, 102, 103]),

    meck:expect(
        kamock_metadata,
        handle_metadata_request,
        kamock_metadata:with_topics([<<"snapshot">>, <<"state">>])
    ),
    PartitionCount = 4,
    meck:expect(
        kamock_metadata_response_topic,
        make_metadata_response_topic,
        kamock_metadata_response_topic:partitions(PartitionCount)
    ),

    meck:expect(kamock_list_offsets_partition_response, make_list_offsets_partition_response, [
        {[<<"snapshot">>, '_', '_'], kamock_list_offsets_partition_response:range(11667, 11668)},
        {[<<"state">>, '_', '_'], kamock_list_offsets_partition_response:range(20156, 20157)},
        {['_', '_', '_'], kamock_list_offsets_partition_response:range(0, 0)}
    ]),

    MessageBuilder = fun(_Topic, Partition, Offset) ->
        Key = iolist_to_binary(io_lib:format("key-~B-~B", [Partition, Offset])),
        Value = iolist_to_binary(io_lib:format("value-~B-~B", [Partition, Offset])),
        #{key => Key, value => Value}
    end,

    meck:expect(kamock_partition_data, make_partition_data, [
        {[<<"snapshot">>, '_', '_'], kamock_partition_data:range(11667, 11668, MessageBuilder)},
        {[<<"state">>, '_', '_'], kamock_partition_data:range(20156, 20157, MessageBuilder)},
        {['_', '_', '_'], kamock_partition_data:empty()}
    ]),

    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun
        (_T = <<"snapshot">>, _P, _O) -> {ok, ?CALLBACK_STATE};
        (_T = <<"state">>, _P, _O) -> {pause, ?CALLBACK_STATE}
    end),
    meck:expect(test_consumer_callback, handle_partition_data, fun
        (
            _T = <<"snapshot">>, P, PartitionData, St
        ) ->
            case kafine_partition_data:at_parity(PartitionData) of
                true ->
                    ok = kafine_consumer:resume(?CONSUMER_REF, <<"state">>, P),
                    {pause, St};
                false ->
                    {ok, St}
            end;
        (_T, _P, _PD, St) ->
            {ok, St}
    end),

    ConnectionOptions = #{},
    GroupId = ?GROUP_ID,
    MembershipOptions = #{},
    ConsumerOptions = #{},
    ParallelHandlerOptions = #{
        callback_mod => test_consumer_callback,
        callback_arg => ?CALLBACK_ARGS,
        skip_empty_fetches => false
    },
    Topics = [<<"snapshot">>, <<"state">>],
    TopicOptions = #{
        <<"snapshot">> => #{offset_reset_policy => latest, initial_offset => -1},
        <<"state">> => #{offset_reset_policy => earliest, initial_offset => 0}
    },
    kafine:start_group_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        ConnectionOptions,
        GroupId,
        MembershipOptions,
        ConsumerOptions,
        ParallelHandlerOptions,
        Topics,
        TopicOptions,
        ?FETCHER_METADATA
    ),

    _ = [
        ?assertWait(
            test_consumer_callback,
            handle_partition_data,
            [<<"snapshot">>, P, meck:is(has_next_offset(11668)), '_'],
            ?WAIT_TIMEOUT_MS
        )
     || P <- [0, 1, 2, 3]
    ],

    _ = [
        ?assertWait(
            test_consumer_callback,
            handle_partition_data,
            [<<"state">>, P, meck:is(has_next_offset(20157)), '_'],
            ?WAIT_TIMEOUT_MS
        )
     || P <- [0, 1, 2, 3]
    ],

    kafine:stop_group_consumer(?CONSUMER_REF),
    kamock_cluster:stop(Cluster),
    ok.

has_next_offset(ExpectedNextOffset) ->
    fun(PartitionData) ->
        kafine_partition_data:next_offset(PartitionData) =:= ExpectedNextOffset
    end.
