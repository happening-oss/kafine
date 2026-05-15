-module(produce_and_consume_SUITE).
-export([all/0, suite/0, single_messages/1, batches/1]).
-include_lib("eunit/include/eunit.hrl").
-export([init/3, handle_partition_data/4]).

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
    ProducerOptions = #{linger_ms => 0},

    % Start a topic consumer
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        ConnectionOptions,
        #{},
        #{assignment_callback => {kafine_noop_assignment_callback, undefined}},
        #{
            callback_mod => produce_and_consume_SUITE,
            callback_arg => {NumMessages, self()}
        },
        Topics,
        #{},
        ?FETCHER_METADATA
    ),

    % And a producer
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Bootstrap, ConnectionOptions, ProducerOptions),

    % Send NumMessages messages to each partition of each topic
    lists:foreach(
        fun(N) ->
            lists:foreach(
                fun(Topic) ->
                    lists:foreach(
                        fun(Partition) ->
                            Msg = make_message(Topic, Partition, N),
                            kafine_producer:produce_sync(?PRODUCER_REF, Topic, Partition, Msg)
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

    NumPartitions = 8,
    MessagesPerBatch = 10,
    NumBatches = 20,

    TopicName1 = ?make_topic_name(1),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName1, NumPartitions, 1),
    TopicName2 = ?make_topic_name(2),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName2, NumPartitions, 1),
    Topics = [TopicName1, TopicName2],

    ConnectionOptions = #{client_id => ?CLIENT_ID},
    ProducerOptions = #{linger_ms => 10},

    % Start a topic consumer
    {ok, _} = kafine:start_topic_consumer(
        ?CONSUMER_REF,
        Bootstrap,
        ConnectionOptions,
        #{},
        #{assignment_callback => {kafine_noop_assignment_callback, undefined}},
        #{
            callback_mod => produce_and_consume_SUITE,
            callback_arg => {NumBatches * MessagesPerBatch, self()}
        },
        Topics,
        #{},
        ?FETCHER_METADATA
    ),

    % And a producer
    {ok, _} = kafine:start_producer(?PRODUCER_REF, Bootstrap, ConnectionOptions, ProducerOptions),

    % Send messages. linger_ms = 10 so these should be grouped into batches
    lists:foreach(
        fun(Batch) ->
            ReqIds =
                lists:foldl(
                    fun(Topic, ReqIdsAcc1) ->
                        lists:foldl(
                            fun(Partition, ReqIdsAcc2) ->
                                lists:foldl(
                                    fun(N, ReqIdsAcc3) ->
                                        Msg = make_message(
                                            Topic, Partition, Batch * MessagesPerBatch + N
                                        ),
                                        Label = {Batch, Topic, Partition, N},
                                        kafine_producer:produce(
                                            ?PRODUCER_REF, Topic, Partition, Msg, Label, ReqIdsAcc3
                                        )
                                    end,
                                    ReqIdsAcc2,
                                    lists:seq(0, MessagesPerBatch - 1)
                                )
                            end,
                            ReqIdsAcc1,
                            lists:seq(0, NumPartitions - 1)
                        )
                    end,
                    kafine_producer:reqids_new(),
                    Topics
                ),

            % wait for the entire batch to be sent and responses to be received
            WaitMs = 2_000,
            Result = kafine_producer:wait_all_responses(ReqIds, WaitMs),
            ExpectedResult =
                #{
                    Topic =>
                        #{
                            Partition => [
                                {{Batch, Topic, Partition, N}, ok}
                             || N <- lists:seq(0, MessagesPerBatch - 1)
                            ]
                         || Partition <- lists:seq(0, NumPartitions - 1)
                        }
                 || Topic <- Topics
                },
            ?assertEqual({ok, ExpectedResult, kafine_producer:reqids_new()}, Result)
        end,
        lists:seq(0, NumBatches - 1)
    ),

    % ensure all topics receive all messages
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

handle_partition_data(
    Topic,
    Partition,
    PartitionData,
    State = #{
        topic := Topic, partition := Partition, test_pid := TestPid
    }
) ->
    {NextState, _} = kafine_partition_data:reduce_while(
        fun(Record = #{offset := Offset}, Acc) ->
            ?assertEqual(
                make_message(Topic, Partition, Offset), maps:with([key, value, headers], Record)
            ),
            {cont, Acc#{next_message => Offset + 1}}
        end,
        State,
        PartitionData
    ),
    case kafine_partition_data:at_parity(PartitionData) of
        true ->
            TestPid ! {done, Topic, Partition};
        _ ->
            ok
    end,
    {ok, NextState}.
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
