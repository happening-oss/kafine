-module(kafine_consumer_offset_commit_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, ?MODULE).
-define(HEARTBEAT_INTERVAL_MS, 30).
-define(WAIT_TIMEOUT_MS, 2_000).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun offset_commit_from_consumer_callback/0
    ]}.

setup() ->
    meck:new(test_consumer_callback, [non_strict]),
    meck:expect(test_consumer_callback, init, fun(_T, _P, _O) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_consumer_callback, begin_record_batch, fun(_T, _P, _O, _Info, St) ->
        {ok, St}
    end),
    meck:expect(test_consumer_callback, handle_record, fun(_T, _P, _M, St) -> {ok, St} end),
    meck:expect(test_consumer_callback, end_record_batch, fun(_T, _P, _N, _Info, St) -> {ok, St} end),

    meck:new(test_assignment_callback, [non_strict]),
    meck:expect(test_assignment_callback, init, fun(_) -> {ok, ?CALLBACK_STATE} end),
    meck:expect(test_assignment_callback, before_assignment, fun(_, _, St) -> {ok, St} end),
    meck:expect(test_assignment_callback, after_assignment, fun(_, _, St) -> {ok, St} end),
    ok.

cleanup(_) ->
    meck:unload().

offset_commit_from_consumer_callback() ->
    % It should be possible to call offset commit from the consumer callback.
    TopicName = ?TOPIC_NAME,
    Partitions = [0, 1, 2, 3],
    Ref = make_ref(),

    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    MessageBuilder = fun(_T, _P, O) ->
        #{key => integer_to_binary(O)}
    end,
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 20, 5, MessageBuilder)
    ),

    ets:new(committed_offsets, [named_table, public]),
    meck:expect(
        kamock_offset_commit,
        handle_offset_commit_request,
        kamock_offset_commit:to_ets(committed_offsets)
    ),
    meck:expect(
        kamock_offset_fetch,
        handle_offset_fetch_request,
        kamock_offset_fetch:from_ets(committed_offsets)
    ),

    % At the end of the each batch, commit the offset.
    meck:expect(
        test_consumer_callback,
        end_record_batch,
        fun(
            Topic, Partition, NextOffset, _Info, St
        ) ->
            % We commit the *next* offset. This is conventional.
            Offsets = #{Topic => #{Partition => NextOffset}},

            % Because the rebalancer is a different process from the consumer, this doesn't deadlock.
            kafine_eager_rebalance:offset_commit(Ref, Offsets),
            {ok, St}
        end
    ),

    {ok, Consumer} = kafine_consumer:start_link(
        ?CONSUMER_REF,
        Broker,
        #{},
        {test_consumer_callback, ?CALLBACK_ARGS},
        #{}
    ),

    GroupId = ?GROUP_ID,
    TopicOptions = #{},
    {ok, Rebalance} = kafine_eager_rebalance:start_link(
        Ref,
        Broker,
        #{},
        GroupId,
        #{
            heartbeat_interval_ms => ?HEARTBEAT_INTERVAL_MS,
            subscription_callback =>
                {kafine_group_consumer_subscription_callback, [
                    Consumer, GroupId, TopicOptions, kafine_group_consumer_offset_callback
                ]},
            assignment_callback => {test_assignment_callback, undefined}
        },
        [TopicName]
    ),

    meck:wait(4, test_consumer_callback, end_record_batch, '_', ?WAIT_TIMEOUT_MS),

    % Did we commit the offsets?
    ?assertEqual(
        [{{GroupId, TopicName, P}, {5, <<>>}} || P <- Partitions],
        lists:sort(ets:tab2list(committed_offsets))
    ),

    kafine_eager_rebalance:stop(Rebalance),
    kafine_consumer:stop(Consumer),
    kamock_broker:stop(Broker),
    ok.
