-module(group_consumer_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        single_member,
        two_members,
        join_later
    ].

suite() ->
    [
        {require, bootstrap_server}
    ].

parse_broker(Broker) when is_list(Broker) ->
    [Host, Port] = string:split(Broker, ":"),
    #{host => list_to_binary(Host), port => list_to_integer(Port)}.

-define(make_topic_name(),
    iolist_to_binary(
        io_lib:format("~s_~s_~s", [?MODULE, ?FUNCTION_NAME, base64url:encode(rand:bytes(6))])
    )
).
-define(make_group_name(),
    iolist_to_binary(
        io_lib:format("group_~s_~s_~s", [?MODULE, ?FUNCTION_NAME, base64url:encode(rand:bytes(6))])
    )
).

-define(CONSUMER_REF, ?FUNCTION_NAME).
-define(make_consumer_ref(Suffix), list_to_atom(atom_to_list(?FUNCTION_NAME) ++ Suffix)).
-define(CONSUMER_REF_1, ?make_consumer_ref("_1")).
-define(CONSUMER_REF_2, ?make_consumer_ref("_2")).
-define(FETCHER_METADATA, #{}).

single_member(_Config) ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, leader]
    ]),

    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    GroupId = ?make_group_name(),

    {ok, _G} = kafine:start_group_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        GroupId,
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        #{},
        #{
            callback_mod => topic_consumer_callback,
            callback_arg => self()
        },
        [TopicName],
        #{},
        ?FETCHER_METADATA
    ),

    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % Produce a message.
    PartitionIndex = 0,
    Key = produce_message(Bootstrap, TopicName, PartitionIndex),

    eventually:assert(records_received(), contains_record(Key)),

    kafine:stop_group_consumer(?CONSUMER_REF),
    ok.

two_members(_Config) ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, leader],
        [kafine, rebalance, follower]
    ]),

    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    GroupId = ?make_group_name(),

    {ok, _Sup1} = kafine:start_group_consumer(
        ?CONSUMER_REF_1,
        Bootstrap,
        #{},
        GroupId,
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        #{},
        #{
            callback_mod => topic_consumer_callback,
            callback_arg => self()
        },
        [TopicName],
        #{},
        ?FETCHER_METADATA
    ),

    {ok, _Sup2} = kafine:start_group_consumer(
        ?CONSUMER_REF_2,
        Bootstrap,
        #{},
        GroupId,
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        #{},
        #{
            callback_mod => topic_consumer_callback,
            callback_arg => self()
        },
        [TopicName],
        #{},
        ?FETCHER_METADATA
    ),

    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    receive
        {[kafine, rebalance, follower], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % TODO: Temporarily disabled. Consider our testing strategy here.
    % The assigned partitions must be unique to each member. No duplicates.
    % eventually:assert(
    %     node_consumer_topic_assignments(TopicName, [?CONSUMER_REF_1, ?CONSUMER_REF_2]),
    %     no_duplicate_partitions()
    % ),

    % Produce a message.
    PartitionIndex = 0,
    Key = produce_message(Bootstrap, TopicName, PartitionIndex),

    eventually:assert(records_received(), contains_record(Key)),

    kafine:stop_group_consumer(?CONSUMER_REF_1),
    kafine:stop_group_consumer(?CONSUMER_REF_2),
    ok.

% If a member joins after the group is stable, there will be a rebalance; do we reassign the partitions correctly?
join_later(_Config) ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, leader],
        [kafine, rebalance, follower]
    ]),

    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    GroupId = ?make_group_name(),

    {ok, _Sup1} = kafine:start_group_consumer(
        ?CONSUMER_REF_1,
        Bootstrap,
        #{},
        GroupId,
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        #{},
        #{
            callback_mod => topic_consumer_callback,
            callback_arg => self()
        },
        [TopicName],
        #{},
        ?FETCHER_METADATA
    ),

    % Wait for the group to become stable (equivalently: C1 is elected leader).
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % Add another member.
    {ok, _Sup2} = kafine:start_group_consumer(
        ?CONSUMER_REF_2,
        Bootstrap,
        #{},
        GroupId,
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        #{},
        #{
            callback_mod => topic_consumer_callback,
            callback_arg => self()
        },
        [TopicName],
        #{},
        ?FETCHER_METADATA
    ),
    % Wait for rebalance
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,
    receive
        {[kafine, rebalance, follower], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % TODO: Temporarily disabled. Consider our testing strategy here.
    % The assigned partitions must be unique to each member. No duplicates.
    % eventually:assert(
    %     node_consumer_topic_assignments(TopicName, [?CONSUMER_REF_1, ?CONSUMER_REF_2]),
    %     no_duplicate_partitions()
    % ),

    % XXX: One recently-fixed bug is that we don't correctly stop the node consumers when revoking partitions. To check
    % for that, we assert that there are no duplicate messages received. But to know that we've received all possible
    % duplicates, that's essentially impossible.

    % I initially considered sending a sentinel message after the normal messages.

    % But it's more complicated than that. If there are duplicate node consumers, they might race, and we'll see Key1,
    % SentinelKey, then Key1, SentinelKey; we'll stop at the first SentinelKey and think there are no duplicates.

    % We can't check broker lag, because that depends on committing offsets.

    % We can't (currently) check that the individual node consumers are at parity (and if we could, that would
    % potentially involve so much poking around in the supervision tree that we'd be able to see the duplicate node
    % consumers anyway).

    % What if we post the first message to partition zero (where the overlap would be), and then post one sentinel
    % message to each of the node consumers (we have their supposed assignments). Then once both of those arrive, we're
    % golden. Unfortunately, that won't work either, because the first buggy node consumer is fetching from that
    % partition as well; this case is identical to partition zero.

    % What about this: We have a topic consumer on partiion zero. When it sees both messages, it produces a third
    % message. Nope: [K1, K2] -> [K1, K2, K3, K1, K2, K3]. It doesn't matter who sends the sentinel. We also have the
    % problem that we don't commit the offsets, so the new node consumer starts from the beginning (and we get
    % duplicates) anyway.

    % The best I've got is to sleep for a while, but that's not great.
    %
    % I thought about waiting for parity and then posting the sentinel (or incrementing/decrementing ETS), but you have
    % the same problem: you're waiting for something to NOT happen.

    kafine:stop_group_consumer(?CONSUMER_REF_1),
    kafine:stop_group_consumer(?CONSUMER_REF_2),
    ok.

% TODO: DRY
produce_message(Bootstrap, TopicName, PartitionIndex) ->
    Key = iolist_to_binary(
        io_lib:format("~s:~B", [?MODULE, erlang:system_time()])
    ),
    MessageLength = 180,
    Value = base64:encode(rand:bytes(MessageLength)),
    Message = #{key => Key, value => Value, headers => []},
    ok = kafka_fixtures:produce_message(Bootstrap, TopicName, PartitionIndex, Message),
    Key.

% TODO: DRY
records_received() ->
    eventually:probe(
        fun Receive(Acc) ->
            receive
                {handle_record, R} ->
                    Receive([R | Acc])
            after 0 ->
                lists:reverse(Acc)
            end
        end,
        [],
        records_received
    ).

contains_record(Expected) ->
    eventually:match(
        fun(Acc) ->
            lists:any(
                fun({_Topic, _Partition, _Message = #{key := Key}}) ->
                    Key =:= Expected
                end,
                Acc
            )
        end,
        {contains_record, Expected}
    ).

node_consumer_topic_assignments(TopicName, Consumers) ->
    eventually:probe(fun() ->
        lists:foldl(
            fun(C, Acc) ->
                #{node_consumers := NodeConsumers} = kafine_consumer:info(C),
                NodePartitionAssignments = maps:fold(
                    fun(_Node, NodeConsumer, Acc1) ->
                        case kafine_node_consumer:info(NodeConsumer) of
                            #{state := _, topic_partitions := #{TopicName := Partitions}} ->
                                [maps:keys(Partitions) | Acc1];
                            _ ->
                                Acc1
                        end
                    end,
                    [],
                    NodeConsumers
                ),
                % If there are any duplicates, they'll appear twice in the resulting list.
                [NodePartitionAssignments | Acc]
            end,
            [],
            Consumers
        )
    end).

no_duplicate_partitions() ->
    eventually:match(fun(AssignedPartitions) ->
        % If there are any duplicates, the uniq-ed list will be shorter than the list-with-duplicates.
        length(lists:uniq(AssignedPartitions)) =:= length(AssignedPartitions)
    end).
