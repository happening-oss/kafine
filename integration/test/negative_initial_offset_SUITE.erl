-module(negative_initial_offset_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("kafcod/include/timestamp.hrl").

all() ->
    [
        against_new_topic,
        against_topic_with_one_message,
        against_empty_non_zero_offset_topic
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
-define(FETCHER_METADATA, #{}).

parse_broker(Broker) when is_list(Broker) ->
    [Host, Port] = string:split(Broker, ":"),
    #{host => list_to_binary(Host), port => list_to_integer(Port)}.

against_new_topic(_Config) ->
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
        #{
            callback_mod => topic_consumer_callback,
            callback_arg => self()
        },
        [TopicName],
        #{TopicName => #{initial_offset => -1, offset_reset_policy => latest}},
        ?FETCHER_METADATA
    ),

    % Produce a message.
    Key = produce_message(Bootstrap, TopicName, PartitionIndex),

    eventually:assert(records_received(), contains_only_record(Key)),

    kafine:stop_topic_consumer(?CONSUMER_REF),
    ok.

against_topic_with_one_message(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    PartitionIndex = 0,
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    % Produce a message before the consumer starts.
    Key1 = produce_message(Bootstrap, TopicName, PartitionIndex),

    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        #{client_id => ?CLIENT_ID},
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        #{
            callback_mod => topic_consumer_callback,
            callback_arg => self()
        },
        [TopicName],
        #{TopicName => #{initial_offset => -1, offset_reset_policy => latest}},
        ?FETCHER_METADATA
    ),

    eventually:assert(records_received(), contains_only_record(Key1)),

    kafine:stop_topic_consumer(?CONSUMER_REF),
    ok.

against_empty_non_zero_offset_topic(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    PartitionIndex = 0,
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    % Produce a bunch of messages.
    InitialCount = 5,
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

    % Then delete all messages.
    DeleteCount = 5,
    ok = kafka_fixtures:delete_records(Bootstrap, TopicName, PartitionIndex, 0, DeleteCount),

    % That should leave us with a non-zero first offset.
    ExpectedFirstOffset = DeleteCount,
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

    % If we start a topic consumer now, we should get an error, because when we subtract one, we fall on an invalid
    % offset, so we should restart from latest. Then when the message is produced, we continue with it.
    ExpectedCount = (InitialCount - DeleteCount),
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        #{client_id => ?CLIENT_ID},
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        #{
            callback_mod => topic_consumer_callback,
            callback_arg => self(),
            skip_empty_fetches => after_first
        },
        [TopicName],
        #{TopicName => #{initial_offset => -1, offset_reset_policy => latest}},
        ?FETCHER_METADATA
    ),

    % We should get OFFSET_OUT_OF_RANGE, then restart from latest. We should get no messages, but we should get an empty
    % batch.

    % Wait until we've fetched at least once.
    eventually:assert(messages_received(), has_end_record_batch()),

    % _Then_ when a message is produced, we should get that message.
    _ = produce_message(Bootstrap, TopicName, PartitionIndex),

    eventually:assert(
        records_received(),
        % The lowest offset should be the expected first offset, and we should see at least the expected number of
        % records.
        eventually:match(fun
            (Records = [{_Topic, _Partition, #{offset := Offset}} | _]) ->
                Offset == ExpectedFirstOffset andalso length(Records) >= ExpectedCount;
            (_) ->
                false
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

messages_received() ->
    eventually:probe(
        fun Receive(Acc) ->
            receive
                M ->
                    Receive([M | Acc])
            after 0 ->
                lists:reverse(Acc)
            end
        end,
        [],
        messages_received
    ).

has_end_record_batch() ->
    eventually:match(
        fun(Messages) ->
            case
                lists:search(
                    fun
                        ({end_record_batch, _}) -> true;
                        (_) -> false
                    end,
                    Messages
                )
            of
                {value, _} -> true;
                false -> false
            end
        end,
        has_end_record_batch
    ).
