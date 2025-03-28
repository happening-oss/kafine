-module(topic_consumer_parity_SUITE).
-compile([export_all, nowarn_export_all]).

all() ->
    [
        virgin_topic,
        emptied_topic,
        earliest_with_messages,
        latest_with_messages
    ].

suite() ->
    [
        {require, bootstrap_server}
    ].

-define(CONSUMER_REF, ?FUNCTION_NAME).
-define(make_topic_name(),
    iolist_to_binary(
        io_lib:format("~s_~s_~s", [?MODULE, ?FUNCTION_NAME, base64url:encode(rand:bytes(6))])
    )
).

parse_broker(Broker) when is_list(Broker) ->
    [Host, Port] = string:split(Broker, ":"),
    #{host => list_to_binary(Host), port => list_to_integer(Port)}.

virgin_topic(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    PartitionIndex = 0,
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    % Start a consumer. We should see parity.
    {ok, History} = topic_consumer_history:start_link(),
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        {topic_consumer_parity_callback, {topic_consumer_history_callback, History}},
        [TopicName],
        #{TopicName => #{}}
    ),

    % TODO: This (and the next one) should assert parity on all partitions at some point.
    eventually:assert(
        eventually:probe(
            fun() ->
                topic_consumer_history:get_history(History, TopicName, PartitionIndex)
            end,
            {history, TopicName, PartitionIndex}
        ),
        eventually:match(
            fun
                (
                    [
                        {_, {parity, _}}
                    ]
                ) ->
                    true;
                (_) ->
                    false
            end,
            parity_only
        )
    ),

    kafine:stop_topic_consumer(?CONSUMER_REF),
    ok.

emptied_topic(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    PartitionIndex = 0,
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    % Add some messages, and then delete them all.
    % TODO: This is potentially distinct from the replication policy deleting them all. Check.
    InitialCount = 20,
    _Keys = [
        produce_message(Bootstrap, TopicName, PartitionIndex)
     || _ <- lists:seq(1, InitialCount)
    ],

    % TODO: If we use InitialCount - 1 (which seems like the correct value), it leaves a message behind. Why?
    kafka_fixtures:delete_records(Bootstrap, TopicName, PartitionIndex, 0, InitialCount),

    % Start a consumer. We should see parity.
    {ok, History} = topic_consumer_history:start_link(),
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        {topic_consumer_parity_callback, {topic_consumer_history_callback, History}},
        [TopicName],
        #{TopicName => #{}}
    ),

    eventually:assert(
        eventually:probe(
            fun() ->
                topic_consumer_history:get_history(History, TopicName, PartitionIndex)
            end,
            {history, TopicName, PartitionIndex}
        ),
        eventually:match(
            fun
                (
                    [
                        {_, {parity, _}}
                    ]
                ) ->
                    true;
                (_) ->
                    false
            end,
            parity_only
        )
    ),

    kafine:stop_topic_consumer(?CONSUMER_REF),
    ok.

earliest_with_messages(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    PartitionIndex = 0,
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    % Produce a message.
    Key1 = produce_message(Bootstrap, TopicName, PartitionIndex),

    % Start a consumer. We should see the message and then parity.
    {ok, History} = topic_consumer_history:start_link(),
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        {topic_consumer_parity_callback, {topic_consumer_history_callback, History}},
        [TopicName],
        #{TopicName => #{}}
    ),

    eventually:assert(
        eventually:probe(
            fun() ->
                topic_consumer_history:get_history(History, TopicName, PartitionIndex)
            end,
            {history, TopicName, PartitionIndex}
        ),
        eventually:match(fun
            (
                [
                    {_, {record, #{key := K}}},
                    {_, {parity, _}}
                ]
            ) when K == Key1 ->
                true;
            (_) ->
                false
        end)
    ),

    topic_consumer_history:reset_history(History),

    % Then produce another message. We should see the message and then parity.
    Key2 = produce_message(Bootstrap, TopicName, PartitionIndex),
    eventually:assert(
        eventually:probe(
            fun() ->
                topic_consumer_history:get_history(History, TopicName, PartitionIndex)
            end,
            {history, TopicName, PartitionIndex}
        ),
        % TODO: These are kinda hard to read, and they're hard to isolate, because of the pattern matching.
        % TODO: Consider a macro, or maybe even a matchspec? Oooh. QLC?
        eventually:match(
            fun
                (
                    [
                        {_, {record, #{key := K}}},
                        {_, {parity, _}}
                    ]
                ) when K == Key2 ->
                    true;
                (_) ->
                    false
            end,
            message_then_parity
        )
    ),

    kafine:stop_topic_consumer(?CONSUMER_REF),
    ok.

latest_with_messages(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = ?make_topic_name(),
    PartitionIndex = 0,
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    % Produce a message.
    _Key = produce_message(Bootstrap, TopicName, PartitionIndex),

    % Start a consumer.
    {ok, History} = topic_consumer_history:start_link(),
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        #{},
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        {topic_consumer_parity_callback, {topic_consumer_history_callback, History}},
        [TopicName],
        #{TopicName => #{initial_offset => latest, offset_reset_policy => latest}}
    ),

    % We should NOT see the message before we see parity.
    eventually:assert(
        eventually:probe(
            fun() ->
                topic_consumer_history:get_history(History, TopicName, PartitionIndex)
            end,
            {history, TopicName, PartitionIndex}
        ),
        eventually:match(
            fun
                ([{{_, _}, {parity, _}}]) ->
                    true;
                (_) ->
                    false
            end,
            just_parity
        )
    ),

    ok = topic_consumer_history:reset_history(History),

    % If we produce another message, we should see the message and then parity.
    _Key2 = produce_message(Bootstrap, TopicName, PartitionIndex),
    eventually:assert(
        eventually:probe(fun() ->
            topic_consumer_history:get_history(History, TopicName, PartitionIndex)
        end),
        eventually:match(
            fun
                (
                    [
                        {{_, _}, {record, _}},
                        {{_, _}, {parity, _}}
                    ]
                ) ->
                    true;
                (_) ->
                    false
            end,
            message_then_parity
        )
    ),

    kafine:stop_topic_consumer(?CONSUMER_REF),
    ok.

% TODO: DRY
produce_message(Bootstrap, TopicName, PartitionIndex) ->
    Key = iolist_to_binary(
        io_lib:format("~s:~s:~B", [?MODULE, ?FUNCTION_NAME, erlang:system_time()])
    ),
    MessageLength = 180,
    Value = base64:encode(rand:bytes(MessageLength)),
    Message = #{key => Key, value => Value, headers => []},
    ok = kafka_fixtures:produce_message(Bootstrap, TopicName, PartitionIndex, Message),
    Key.
