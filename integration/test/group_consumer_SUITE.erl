-module(group_consumer_SUITE).
-export([all/0, suite/0, single_member/1, two_members/1, join_later/1]).
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
        #{assignment_callback => {kafine_noop_assignment_callback, undefined}},
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
        #{assignment_callback => {kafine_noop_assignment_callback, undefined}},
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
        #{assignment_callback => {kafine_noop_assignment_callback, undefined}},
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
        #{assignment_callback => {kafine_noop_assignment_callback, undefined}},
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
        #{assignment_callback => {kafine_noop_assignment_callback, undefined}},
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
                {handle_partition_data, {_T, _P, PD}} ->
                    {Records, _} = kafine_partition_data:flatten(PD),
                    Receive(Acc ++ Records)
            after 0 ->
                Acc
            end
        end,
        [],
        records_received
    ).

contains_record(Expected) ->
    eventually:match(
        fun(Acc) ->
            lists:any(
                fun(#{key := Key}) ->
                    Key =:= Expected
                end,
                Acc
            )
        end,
        {contains_record, Expected}
    ).
