-module(kafine_node_consumer_give_away_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(WAIT_TIMEOUT_MS, 2_000).

setup() ->
    kafine_node_consumer_tests:setup(?MODULE).

cleanup(_) ->
    meck:unload().

kafine_node_consumer_give_away_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun not_leader_or_follower/0,
        fun not_leader_or_follower_2/0
    ]}.

not_leader_or_follower() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % When we see a fetch, return ?NOT_LEADER_OR_FOLLOWER.
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        fun(_, #{partition := P}, _) ->
            kamock_partition_data:make_error(P, ?NOT_LEADER_OR_FOLLOWER)
        end
    ),

    % We expect to see a *single* fetch; any further fetches are an error (because we're not the leader; duh).
    meck:expect(
        kamock_fetch,
        handle_fetch_request,
        ['_', '_'],
        meck:seq(
            [
                meck:passthrough(),
                meck:raise(error, unexpected)
            ]
        )
    ),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            61 => #{}
        }
    }),
    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % The node consumer should notify its owner (us) that it lost the partition.
    receive
        {give_away, _} ->
            ok
    after ?WAIT_TIMEOUT_MS ->
        error(timeout)
    end,

    % We should be in the 'idle' state -- we've got nothing to do, so we don't bother issuing empty fetches.
    ?assertMatch({idle, _}, sys:get_state(Pid)),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

not_leader_or_follower_2() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % When we see a fetch on partition 61, return ?NOT_LEADER_OR_FOLLOWER. Otherwise, pass through.
    meck:expect(
        kamock_partition_data,
        make_partition_data,
        fun
            (_, #{partition := P}, _) when P =:= 61 ->
                kamock_partition_data:make_error(P, ?NOT_LEADER_OR_FOLLOWER);
            (Topic, FetchPartition, Env) ->
                meck:passthrough([Topic, FetchPartition, Env])
        end
    ),

    TopicName = ?TOPIC_NAME,
    TopicPartitionStates = init_topic_partition_states(#{
        TopicName => #{
            61 => #{},
            62 => #{}
        }
    }),
    {ok, Pid} = start_node_consumer(?CONSUMER_REF, Broker, TopicPartitionStates),

    % The node consumer should notify its owner (us) that it lost partition zero.
    receive
        {give_away, GiveAway} ->
            #{TopicName := Partitions} = GiveAway,
            ?assertEqual(1, map_size(Partitions)),
            ?assertMatch(#{61 := _}, Partitions),
            ok
    after ?WAIT_TIMEOUT_MS ->
        error(timeout)
    end,

    % We expect to have seen a fetch for partitions 61 and 62, then another one for just partition 62.
    meck:wait(2, kamock_fetch, handle_fetch_request, '_', ?WAIT_TIMEOUT_MS),
    [
        % In reverse order; implementation detail.
        {_, {_, _, [#{topics := [#{partitions := [#{partition := 62}, #{partition := 61}]}]}, #{}]},
            _},
        {_, {_, _, [#{topics := [#{partitions := [#{partition := 62}]}]}, #{}]}, _}
    ] = meck:history(kamock_fetch),

    stop_node_consumer(Pid, TopicPartitionStates),
    kamock_broker:stop(Broker),
    ok.

init_topic_partition_states(InitStates) ->
    kafine_fetch_response_tests:init_topic_partition_states(InitStates).

start_node_consumer(Ref, Broker, TopicPartitionStates) ->
    kafine_node_consumer_tests:start_node_consumer(Ref, Broker, TopicPartitionStates).

stop_node_consumer(Pid, TopicPartitionStates) ->
    kafine_node_consumer_tests:stop_node_consumer(Pid, TopicPartitionStates).
