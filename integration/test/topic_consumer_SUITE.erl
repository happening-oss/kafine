-module(topic_consumer_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("kafcod/include/timestamp.hrl").

all() ->
    [
        single_topic,
        latest_offset,
        nonzero_offset
    ].

suite() ->
    [
        {require, bootstrap_server}
    ].

-define(CLIENT_ID, atom_to_binary(?MODULE)).

-define(CONSUMER_REF, ?FUNCTION_NAME).
-define(make_topic_name(),
    iolist_to_binary(
        io_lib:format("~s_~s_~s", [?MODULE, ?FUNCTION_NAME, base64url:encode(rand:bytes(6))])
    )
).

parse_broker(Broker) when is_list(Broker) ->
    [Host, Port] = string:split(Broker, ":"),
    #{host => list_to_binary(Host), port => list_to_integer(Port)}.

single_topic(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    PartitionIndex = 0,
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        #{client_id => ?CLIENT_ID},
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        {topic_consumer_callback, self()},
        [TopicName],
        #{TopicName => #{}}
    ),

    % Produce a message.
    Key = produce_message(Bootstrap, TopicName, PartitionIndex),

    eventually:assert(records_received(), contains_record(Key)),

    kafine:stop_topic_consumer(?CONSUMER_REF),
    ok.

latest_offset(_Config) ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, node_consumer, connected]
    ]),

    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    PartitionIndex = 0,
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    % Produce a message before the consumer starts.
    _Key1 = produce_message(Bootstrap, TopicName, PartitionIndex),

    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        #{client_id => ?CLIENT_ID},
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        {topic_consumer_callback, self()},
        [TopicName],
        #{TopicName => #{offset_reset_policy => latest}}
    ),
    % Give us some time to allow the consumer to start before producing another message
    receive
        {[kafine, node_consumer, connected], TelemetryRef, #{}, #{}} -> ok
    end,

    % Produce another message.
    Key2 = produce_message(Bootstrap, TopicName, PartitionIndex),

    eventually:assert(records_received(), contains_only_record(Key2)),

    kafine:stop_topic_consumer(?CONSUMER_REF),
    ok.

nonzero_offset(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    PartitionIndex = 0,
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    % Produce a bunch of messages.
    InitialCount = 20,
    _Keys = [
        produce_message(Bootstrap, TopicName, PartitionIndex)
     || _ <- lists:seq(1, InitialCount)
    ],

    % Connect to the leader for that partition.
    Leader = kafka_fixtures:get_leader(Bootstrap, TopicName, PartitionIndex),
    {ok, L} = kafine_connection:start_link(Leader, #{client_id => ?CLIENT_ID}),

    % Wait until those messages exist.
    eventually:assert(
        eventually:probe(fun() ->
            kafka_topic_assert:get_message_count(L, TopicName, PartitionIndex) == InitialCount
        end)
    ),

    % Then delete some of those messages.
    DeleteCount = 10,
    ok = kafka_fixtures:delete_records(Bootstrap, TopicName, PartitionIndex, 0, DeleteCount - 1),

    % That should leave us with a non-zero first offset.
    ExpectedFirstOffset = (InitialCount - DeleteCount - 1),
    ExpectedCount = (InitialCount - DeleteCount),
    eventually:assert(
        eventually:probe(
            fun() ->
                kafka_topic_assert:get_offset_of_topic_partition(
                    L, TopicName, PartitionIndex, ?EARLIEST_TIMESTAMP
                )
            end
        ),
        eventually:match(
            fun(FirstOffset) ->
                FirstOffset == ExpectedFirstOffset
            end
        )
    ),

    % If we start a topic consumer now, we should start from the correct first offset.
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        {topic_consumer_callback, self()},
        [TopicName],
        #{TopicName => #{offset_reset_policy => earliest}}
    ),

    eventually:assert(
        records_received(),
        % The lowest offset should be the expected first offset, and we should see at least the expected number of
        % records.
        eventually:match(fun(Records = [{_Topic, _Partition, #{offset := Offset}} | _]) ->
            Offset == ExpectedFirstOffset andalso length(Records) >= ExpectedCount
        end)
    ),

    kafine:stop_topic_consumer(?CONSUMER_REF),
    ok.

produce_message(Bootstrap, TopicName, PartitionIndex) ->
    Key = iolist_to_binary(
        io_lib:format("~s:~s:~B", [?MODULE, ?FUNCTION_NAME, erlang:system_time()])
    ),
    MessageLength = 180,
    Value = base64:encode(rand:bytes(MessageLength)),
    Message = #{key => Key, value => Value, headers => []},
    ok = kafka_fixtures:produce_message(Bootstrap, TopicName, PartitionIndex, Message),
    Key.

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

contains_only_record(Expected) ->
    eventually:match(
        fun
            ([]) ->
                error(no_records);
            ([{_Topic, _Partition, _Message = #{key := Key}}]) when Key =:= Expected ->
                true;
            ([_ | _]) ->
                false
        end,
        {contains_only_record, Expected}
    ).
