-module(produce_and_consume_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-define(CONSUMER_REF, {?FUNCTION_NAME, consumer}).
-define(PRODUCER_REF, {?FUNCTION_NAME, producer}).
-define(CLIENT_ID, atom_to_binary(?MODULE)).
-define(make_topic_name(N),
    iolist_to_binary(
        io_lib:format("~s_~s_~s_~B", [?MODULE, ?FUNCTION_NAME, base64url:encode(rand:bytes(6)), N])
    )
).
-define(FETCHER_METADATA, #{}).

all() ->
    [
        single_messages,
        batches
    ].

suite() ->
    [
        {require, bootstrap_server}
    ].

single_messages(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    NumPartitions = 3,
    NumMessages = 5,

    TopicName1 = ?make_topic_name(1),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName1, NumPartitions, 1),
    TopicName2 = ?make_topic_name(2),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName2, NumPartitions, 1),
    Topics = [TopicName1, TopicName2],

    ConnectionOptions = #{client_id => ?CLIENT_ID},

    % Start a topic consumer
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        ConnectionOptions,
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        #{
            callback_mod => produce_and_consume_SUITE,
            callback_arg => {NumMessages, self()}
        },
        Topics,
        #{},
        ?FETCHER_METADATA
    ),

    % And a producer
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Bootstrap, ConnectionOptions),

    % Send NumMessages messages to each partition of each topic
    lists:foreach(
        fun(N) ->
            lists:foreach(
                fun(Topic) ->
                    lists:foreach(
                        fun(Partition) ->
                            Msg = make_message(Topic, Partition, N),
                            kafine_producer:produce(?PRODUCER_REF, Topic, Partition, #{}, #{}, [Msg])
                        end,
                        lists:seq(0, NumPartitions - 1)
                    )
                end,
                Topics
            )
        end,
        lists:seq(0, NumMessages - 1)
    ),

    % wait for each topic partition to receive all messages
    lists:foreach(
        fun(Topic) ->
            lists:foreach(
                fun(Partition) ->
                    receive
                        {done, Topic, Partition} -> ok
                    after 10_000 ->
                        error({timeout, Topic, Partition})
                    end
                end,
                lists:seq(0, NumPartitions - 1)
            )
        end,
        Topics
    ),

    kafine:stop_producer(?PRODUCER_REF),
    kafine:stop_topic_consumer(?CONSUMER_REF),

    ok.

batches(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    NumPartitions = 3,
    MessagesPerBatch = 3,
    NumBatches = 5,

    TopicName1 = ?make_topic_name(1),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName1, NumPartitions, 1),
    TopicName2 = ?make_topic_name(2),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName2, NumPartitions, 1),
    Topics = [TopicName1, TopicName2],

    ConnectionOptions = #{client_id => ?CLIENT_ID},

    % Start a topic consumer
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        ConnectionOptions,
        #{},
        #{assignment_callback => {do_nothing_assignment_callback, undefined}},
        #{
            callback_mod => produce_and_consume_SUITE,
            callback_arg => {NumBatches * MessagesPerBatch, self()}
        },
        Topics,
        #{},
        ?FETCHER_METADATA
    ),

    % And a producer
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Bootstrap, ConnectionOptions),

    % Send batches
    lists:foreach(
        fun(Batch) ->
            Messages = #{
                Topic =>
                    #{
                        Partition => [
                            make_message(Topic, Partition, Batch * MessagesPerBatch + N)
                         || N <- lists:seq(0, MessagesPerBatch - 1)
                        ]
                     || Partition <- lists:seq(0, NumPartitions - 1)
                    }
             || Topic <- Topics
            },
            kafine_producer:produce_batch(?PRODUCER_REF, #{}, Messages, #{})
        end,
        lists:seq(0, NumBatches - 1)
    ),

    % wait for each topic partition to receive all messages
    lists:foreach(
        fun(Topic) ->
            lists:foreach(
                fun(Partition) ->
                    receive
                        {done, Topic, Partition} -> ok
                    after 10_000 ->
                        error({timeout, Topic, Partition})
                    end
                end,
                lists:seq(0, NumPartitions - 1)
            )
        end,
        Topics
    ),

    kafine:stop_producer(?PRODUCER_REF),
    kafine:stop_topic_consumer(?CONSUMER_REF),

    ok.

%%% consumer callback
init(Topic, Partition, {NumMessages, TestPid}) ->
    {ok, #{
        topic => Topic,
        partition => Partition,
        num_messages => NumMessages,
        test_pid => TestPid,
        next_message => 0
    }}.

begin_record_batch(
    Topic,
    Partition,
    FetchOffset,
    _,
    State = #{topic := Topic, partition := Partition, next_message := FetchOffset}
) ->
    {ok, State}.

handle_record(
    Topic,
    Partition,
    Message = #{offset := Offset},
    State = #{
        topic := Topic, partition := Partition, num_messages := NumMessages, next_message := Offset
    }
) when Offset < NumMessages ->
    ?assertEqual(make_message(Topic, Partition, Offset), maps:with([key, value, headers], Message)),
    {ok, State#{next_message => Offset + 1}}.

end_record_batch(
    Topic,
    Partition,
    NextOffset,
    _,
    State = #{
        topic := Topic,
        partition := Partition,
        num_messages := NextOffset,
        test_pid := TestPid,
        next_message := NextOffset
    }
) ->
    TestPid ! {done, Topic, Partition},
    {ok, State};
end_record_batch(
    Topic,
    Partition,
    NextOffset,
    _,
    State = #{
        topic := Topic,
        partition := Partition,
        num_messages := NumRecords,
        next_message := NextOffset
    }
) when NextOffset < NumRecords ->
    {ok, State}.
%%% end consumer callback

parse_broker(Broker) when is_list(Broker) ->
    [Host, Port] = string:split(Broker, ":"),
    #{host => list_to_binary(Host), port => list_to_integer(Port)}.

make_message(Topic, Partition, N) ->
    Key = iolist_to_binary(io_lib:format("key_~s_~B_~B", [Topic, Partition, N])),
    Value = iolist_to_binary(io_lib:format("value_~s_~B_~B", [Topic, Partition, N])),
    #{
        key => Key,
        value => Value,
        headers => []
    }.
