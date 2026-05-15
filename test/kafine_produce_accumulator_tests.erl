-module(kafine_produce_accumulator_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kafine_eqwalizer.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("assert_received.hrl").

-define(PRODUCER_OPTIONS, #{
    max_batch_size_bytes => 16_384,
    max_request_size_bytes => 1_048_576
}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(TOPIC_NAME_2, iolist_to_binary(io_lib:format("~s___~s_t_2", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 1).
-define(PARTITION_2, 2).
-define(PARTITION_3, 3).
-define(TIMESTAMP, 1_000_000_000).
-define(FROM(X), ?DYNAMIC_CAST({from, X})).

kafine_produce_accumulator_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun collect_from_empty_returns_no_messages/0,
        fun append_gather_collect/0,
        fun collect_with_allow_gather/0,
        fun collect_not_appended_partition_returns_no_messages/0,
        fun append_multiple/0,
        fun gather_multiple_times_creates_separate_batches_collected_one_at_a_time/0,
        fun multiple_partitions/0,
        fun multiple_topics/0,
        fun collect_only_collects_specified_partitions/0,
        fun collect_takes_queued_batches_first/0,
        fun gathers_due_to_batch_size/0,
        fun message_exceeding_max_batch_size_is_gathered_immediately/0,
        fun message_exceeding_max_batch_size_gathers_current_batch_before_itself/0,
        fun requeue_batches/0,
        fun requeued_batches_are_collected_first/0,
        fun rejects_single_message_exceeding_max_request_size/0,
        fun only_collects_batches_up_to_max_request_size/0,
        fun collect_with_gather_prefers_completed_batches/0,
        fun busy_partition_can_not_starve_other_partitions/0,

        fun creates_telemetry_events/0,
        fun info/0,

        fun fair_reduce_while_from_first/0,
        fun fair_reduce_while_from_middle/0,
        fun fair_reduce_while_from_last/0
    ]}.

setup() ->
    meck:new(rand, [unstick, passthrough]),
    meck:expect(rand, uniform, fun(_) -> 1 end).

cleanup(_) ->
    meck:unload().

collect_from_empty_returns_no_messages() ->
    Topic = ?TOPIC_NAME,

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),

    TopicPartitions = #{Topic => [?PARTITION_1]},
    ?assertEqual(
        no_messages, kafine_produce_accumulator:collect_request(TopicPartitions, true, State0)
    ).

append_gather_collect() ->
    Topic = ?TOPIC_NAME,
    Message = message(#{key => <<"key1">>, value => <<"value1">>}),
    From = ?FROM(test1),

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message,
        From,
        State0
    ),

    State2 = kafine_produce_accumulator:gather_batch(Topic, ?PARTITION_1, State1),

    TopicPartitions = #{Topic => [?PARTITION_1]},
    {#{Topic := #{?PARTITION_1 := {[{Message, From}], _}}}, [], _, State3} =
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State2),

    ?assertEqual(
        no_messages, kafine_produce_accumulator:collect_request(TopicPartitions, true, State3)
    ).

collect_with_allow_gather() ->
    Topic = ?TOPIC_NAME,
    Message = message(#{key => <<"key1">>, value => <<"value1">>}),
    From = ?FROM(test1),

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message,
        From,
        State0
    ),

    TopicPartitions = #{Topic => [?PARTITION_1]},
    {
        #{Topic := #{?PARTITION_1 := {[{Message, From}], _}}},
        [{Topic, ?PARTITION_1}],
        _,
        State2
    } =
        kafine_produce_accumulator:collect_request(TopicPartitions, true, State1),

    ?assertEqual(
        no_messages, kafine_produce_accumulator:collect_request(TopicPartitions, true, State2)
    ).

collect_not_appended_partition_returns_no_messages() ->
    Topic = ?TOPIC_NAME,
    Message = message(#{key => <<"key1">>, value => <<"value1">>}),
    From = ?FROM(test1),

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message,
        From,
        State0
    ),

    TopicPartitions = #{Topic => [?PARTITION_2]},
    ?assertEqual(
        no_messages, kafine_produce_accumulator:collect_request(TopicPartitions, true, State1)
    ).

append_multiple() ->
    Topic = ?TOPIC_NAME,
    Count = 10,
    Messages = [
        message(#{key => iolist_to_binary(io_lib:format("key~B", [N]))})
     || N <- lists:seq(1, Count)
    ],
    Froms = [?FROM(N) || N <- lists:seq(1, Count)],
    Batch = lists:zip(Messages, Froms),

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),

    {_, State1} = lists:foldl(
        fun({Message, From}, {First, State}) ->
            {ok, First, false, NewState} = kafine_produce_accumulator:append(
                Topic,
                ?PARTITION_1,
                Message,
                From,
                State
            ),
            {false, NewState}
        end,
        {true, State0},
        Batch
    ),

    State2 = kafine_produce_accumulator:gather_batch(Topic, ?PARTITION_1, State1),

    TopicPartitions = #{Topic => [?PARTITION_1]},
    ?assertMatch(
        {#{Topic := #{?PARTITION_1 := {Batch, _}}}, [], _, _},
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State2)
    ).

gather_multiple_times_creates_separate_batches_collected_one_at_a_time() ->
    Topic = ?TOPIC_NAME,
    Count = 10,

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),

    State1 = lists:foldl(
        fun(N, State) ->
            Message = message(#{key => iolist_to_binary(io_lib:format("key~B", [N]))}),
            From = ?FROM(N),
            {ok, true, false, State01} = kafine_produce_accumulator:append(
                Topic,
                ?PARTITION_1,
                Message,
                From,
                State
            ),
            kafine_produce_accumulator:gather_batch(Topic, ?PARTITION_1, State01)
        end,
        State0,
        lists:seq(1, Count)
    ),

    TopicPartitions = #{Topic => [?PARTITION_1]},
    lists:foldl(
        fun(N, State) ->
            ExpectedMessage = message(#{key => iolist_to_binary(io_lib:format("key~B", [N]))}),
            ExpectedFrom = ?FROM(N),
            {#{Topic := #{?PARTITION_1 := {[{ExpectedMessage, ExpectedFrom}], _}}}, [], _, NewState} =
                kafine_produce_accumulator:collect_request(TopicPartitions, false, State),
            NewState
        end,
        State1,
        lists:seq(1, Count)
    ).

multiple_partitions() ->
    Topic = ?TOPIC_NAME,
    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    Message2 = message(#{key => <<"key2">>, value => <<"value2">>}),
    From2 = ?FROM(test2),

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    {ok, true, false, State2} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_2,
        Message2,
        From2,
        State1
    ),

    TopicPartitions = #{Topic => [?PARTITION_1, ?PARTITION_2, ?PARTITION_3]},
    ?assertMatch(
        {
            #{
                Topic := #{
                    ?PARTITION_1 := {[{Message1, From1}], _},
                    ?PARTITION_2 := {[{Message2, From2}], _}
                }
            },
            [{Topic, ?PARTITION_2}, {Topic, ?PARTITION_1}],
            _,
            _
        },
        kafine_produce_accumulator:collect_request(TopicPartitions, true, State2)
    ).

multiple_topics() ->
    Topic1 = ?TOPIC_NAME,
    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    Topic2 = ?TOPIC_NAME_2,
    Message2 = message(#{key => <<"key2">>, value => <<"value2">>}),
    From2 = ?FROM(test2),

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic1,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    {ok, true, false, State2} = kafine_produce_accumulator:append(
        Topic2,
        ?PARTITION_2,
        Message2,
        From2,
        State1
    ),

    TopicPartitions = #{Topic1 => [?PARTITION_1], Topic2 => [?PARTITION_2]},
    ?assertMatch(
        {
            #{
                Topic1 := #{?PARTITION_1 := {[{Message1, From1}], _}},
                Topic2 := #{?PARTITION_2 := {[{Message2, From2}], _}}
            },
            [{Topic2, ?PARTITION_2}, {Topic1, ?PARTITION_1}],
            _,
            _
        },
        kafine_produce_accumulator:collect_request(TopicPartitions, true, State2)
    ).

collect_only_collects_specified_partitions() ->
    Topic = ?TOPIC_NAME,
    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    Message2 = message(#{key => <<"key2">>, value => <<"value2">>}),
    From2 = ?FROM(test2),

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    {ok, true, false, State2} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_2,
        Message2,
        From2,
        State1
    ),

    TopicPartitions = #{Topic => [?PARTITION_1]},
    {#{Topic := #{?PARTITION_1 := {[{Message1, From1}], _}}}, [{Topic, ?PARTITION_1}], _, State3} =
        kafine_produce_accumulator:collect_request(TopicPartitions, true, State2),

    % Ensure the other partition hasn't been gathered
    TopicPartitions2 = #{Topic => [?PARTITION_2]},
    ?assertEqual(
        no_messages,
        kafine_produce_accumulator:collect_request(TopicPartitions2, false, State3)
    ),

    % Ensure the other partition can still be collected
    ?assertMatch(
        {#{Topic := #{?PARTITION_2 := {[{Message2, From2}], _}}}, [{Topic, ?PARTITION_2}], _, _},
        kafine_produce_accumulator:collect_request(TopicPartitions2, true, State3)
    ).

collect_takes_queued_batches_first() ->
    Topic = ?TOPIC_NAME,
    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    Message2 = message(#{key => <<"key2">>, value => <<"value2">>}),
    From2 = ?FROM(test2),

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    State2 = kafine_produce_accumulator:gather_batch(Topic, ?PARTITION_1, State1),

    {ok, true, false, State3} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message2,
        From2,
        State2
    ),

    TopicPartitions = #{Topic => [?PARTITION_1]},
    {#{Topic := #{?PARTITION_1 := {[{Message1, From1}], _}}}, [], _, State4} =
        kafine_produce_accumulator:collect_request(TopicPartitions, true, State3),

    % Ensure the other message hasn't been gathered
    TopicPartitions2 = #{Topic => [?PARTITION_1]},
    ?assertEqual(
        no_messages,
        kafine_produce_accumulator:collect_request(TopicPartitions2, false, State4)
    ),

    % Ensure the other partition can still be collected
    ?assertMatch(
        {#{Topic := #{?PARTITION_1 := {[{Message2, From2}], _}}}, [{Topic, ?PARTITION_1}], _, _},
        kafine_produce_accumulator:collect_request(TopicPartitions2, true, State4)
    ).

gathers_due_to_batch_size() ->
    Topic = ?TOPIC_NAME,

    ProducerOptions0 = ?PRODUCER_OPTIONS,
    % Size of each message ends up being 17 bytes, + 61 byte overhead for batch. This holds 2 messages max
    ProducerOptions = ProducerOptions0#{max_batch_size_bytes => 100},
    State0 = kafine_produce_accumulator:init(ProducerOptions, #{}),

    % First message fits in batch
    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    % Second message also fits
    Message2 = message(#{key => <<"key2">>, value => <<"value2">>}),
    From2 = ?FROM(test2),
    {ok, false, false, State2} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message2,
        From2,
        State1
    ),

    % Third message won't fit
    Message3 = message(#{key => <<"key3">>, value => <<"value3">>}),
    From3 = ?FROM(test3),
    {ok, true, true, State3} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message3,
        From3,
        State2
    ),

    % Fourth message should end up in batch with third
    Message4 = message(#{key => <<"key4">>, value => <<"value4">>}),
    From4 = ?FROM(test4),
    {ok, false, false, State4} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message4,
        From4,
        State3
    ),

    TopicPartitions = #{Topic => [?PARTITION_1]},
    {
        #{Topic := #{?PARTITION_1 := {[{Message1, From1}, {Message2, From2}], _}}},
        [],
        _,
        State5
    } =
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State4),
    {
        #{Topic := #{?PARTITION_1 := {[{Message3, From3}, {Message4, From4}], _}}},
        [{Topic, ?PARTITION_1}],
        _,
        _
    } =
        kafine_produce_accumulator:collect_request(TopicPartitions, true, State5).

message_exceeding_max_batch_size_is_gathered_immediately() ->
    Topic = ?TOPIC_NAME,

    ProducerOptions0 = ?PRODUCER_OPTIONS,
    % Size of each message ends up being 17 bytes, + 61 byte overhead for batch. This holds 2 messages max
    ProducerOptions = ProducerOptions0#{max_batch_size_bytes => 100},
    State0 = kafine_produce_accumulator:init(ProducerOptions, #{}),

    LongValue =
        <<"a long value which will cause the record to exceed the maximum batch size when produced on its own">>,

    Message1 = message(#{key => <<"key1">>, value => LongValue}),
    From1 = ?FROM(test1),
    % Note this returns NewBatch = false. NewBatch indicates that a new incomplete batch has been
    % created, which is not the case here as the batch is immediately gathered
    {ok, false, true, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    TopicPartitions = #{Topic => [?PARTITION_1]},
    ?assertMatch(
        {
            #{Topic := #{?PARTITION_1 := {[{Message1, From1}], _}}},
            [],
            _,
            _
        },
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State1)
    ).

message_exceeding_max_batch_size_gathers_current_batch_before_itself() ->
    Topic = ?TOPIC_NAME,

    ProducerOptions0 = ?PRODUCER_OPTIONS,
    % Size of each message ends up being 17 bytes, + 61 byte overhead for batch. This holds 2 messages max
    ProducerOptions = ProducerOptions0#{max_batch_size_bytes => 100},
    State0 = kafine_produce_accumulator:init(ProducerOptions, #{}),

    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    LongValue =
        <<"a long value which will cause the record to exceed the maximum batch size when produced on its own">>,
    Message2 = message(#{key => <<"key1">>, value => LongValue}),
    From2 = ?FROM(test1),
    % Note this returns NewBatch = false. NewBatch indicates that a new incomplete batch has been
    % created, which is not the case here as the batch is immediately gathered
    {ok, false, true, State2} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message2,
        From2,
        State1
    ),

    TopicPartitions = #{Topic => [?PARTITION_1]},
    {
        #{Topic := #{?PARTITION_1 := {[{Message1, From1}], _}}},
        [],
        _,
        State3
    } =
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State2),
    {
        #{Topic := #{?PARTITION_1 := {[{Message2, From2}], _}}},
        [],
        _,
        _
    } =
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State3).

requeue_batches() ->
    Topic = ?TOPIC_NAME,
    Topic2 = ?TOPIC_NAME_2,

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),

    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    Message2 = message(#{key => <<"key2">>, value => <<"value2">>}),
    From2 = ?FROM(test2),
    {ok, true, false, State2} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_2,
        Message2,
        From2,
        State1
    ),

    Message3 = message(#{key => <<"key3">>, value => <<"value3">>}),
    From3 = ?FROM(test3),
    {ok, true, false, State3} = kafine_produce_accumulator:append(
        Topic2,
        ?PARTITION_1,
        Message3,
        From3,
        State2
    ),

    % Collect our 3 messages from multiple topics and partitions
    TopicPartitions = #{
        Topic => [?PARTITION_1, ?PARTITION_2],
        Topic2 => [?PARTITION_1, ?PARTITION_2]
    },

    {
        Request = #{
            Topic := #{
                ?PARTITION_1 := {[{Message1, From1}], _},
                ?PARTITION_2 := {[{Message2, From2}], _}
            },
            Topic2 := #{
                ?PARTITION_1 := {[{Message3, From3}], _}
            }
        },
        _,
        _,
        State4
    } =
        kafine_produce_accumulator:collect_request(TopicPartitions, true, State3),

    % Add another message on a new topic/partition
    Message4 = message(#{key => <<"key4">>, value => <<"value4">>}),
    From4 = ?FROM(test),
    {ok, true, false, State5} = kafine_produce_accumulator:append(
        Topic2,
        ?PARTITION_2,
        Message4,
        From4,
        State4
    ),

    % requeue the collected request
    State6 = kafine_produce_accumulator:requeue_batches(Request, State5),

    % Collect again, should have all 4 messages
    ?assertMatch(
        {
            #{
                Topic := #{
                    ?PARTITION_1 := {[{Message1, From1}], _},
                    ?PARTITION_2 := {[{Message2, From2}], _}
                },
                Topic2 := #{
                    ?PARTITION_1 := {[{Message3, From3}], _},
                    ?PARTITION_2 := {[{Message4, From4}], _}
                }
            },
            _,
            _,
            _
        },
        kafine_produce_accumulator:collect_request(TopicPartitions, true, State6)
    ).

requeued_batches_are_collected_first() ->
    Topic = ?TOPIC_NAME,

    ProducerOptions0 = ?PRODUCER_OPTIONS,
    % Automatically collect every message into its own batch
    ProducerOptions = ProducerOptions0#{max_batch_size_bytes => 0},
    State0 = kafine_produce_accumulator:init(ProducerOptions, #{}),

    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    {ok, false, true, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    TopicPartitions = #{Topic => [?PARTITION_1]},

    {Request = #{Topic := #{?PARTITION_1 := {[{Message1, From1}], _}}}, [], _, State2} =
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State1),

    Message2 = message(#{key => <<"key2">>, value => <<"value2">>}),
    From2 = ?FROM(test2),
    {ok, false, true, State3} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message2,
        From2,
        State2
    ),

    State4 = kafine_produce_accumulator:requeue_batches(Request, State3),

    {#{Topic := #{?PARTITION_1 := {[{Message1, From1}], _}}}, [], _, State5} =
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State4),

    {#{Topic := #{?PARTITION_1 := {[{Message2, From2}], _}}}, [], _, _} =
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State5).

rejects_single_message_exceeding_max_request_size() ->
    Topic = ?TOPIC_NAME,

    ProducerOptions0 = ?PRODUCER_OPTIONS,
    ProducerOptions = ProducerOptions0#{max_request_size_bytes => 100},

    LongValue =
        <<"a long value which will cause the record to exceed the maximum request size when produced on its own">>,

    Message = message(#{key => <<"key1">>, value => LongValue}),
    From = ?FROM(test1),
    ?assertEqual(
        {error, message_too_large},
        kafine_produce_accumulator:append(
            Topic,
            ?PARTITION_1,
            Message,
            From,
            kafine_produce_accumulator:init(ProducerOptions, #{})
        )
    ).

only_collects_batches_up_to_max_request_size() ->
    Topic = ?TOPIC_NAME,

    ProducerOptions0 = ?PRODUCER_OPTIONS,
    ProducerOptions = ProducerOptions0#{
        % Always gather single message batches
        max_batch_size_bytes := 0,
        % Can only fit two batches in a request
        max_request_size_bytes := 200
    },
    State0 = kafine_produce_accumulator:init(ProducerOptions, #{}),

    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    {ok, false, true, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    Message2 = message(#{key => <<"key2">>, value => <<"value2">>}),
    From2 = ?FROM(test2),
    {ok, false, true, State2} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_2,
        Message2,
        From2,
        State1
    ),

    Message3 = message(#{key => <<"key3">>, value => <<"value3">>}),
    From3 = ?FROM(test3),
    {ok, false, true, State3} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_3,
        Message3,
        From3,
        State2
    ),

    TopicPartitions = #{Topic => [?PARTITION_1, ?PARTITION_2, ?PARTITION_3]},
    {Collected1, [], _, State4} =
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State3),
    ?assertMatch(
        #{
            Topic :=
                #{
                    ?PARTITION_1 := {[{Message1, From1}], _},
                    ?PARTITION_2 := {[{Message2, From2}], _}
                }
        },
        Collected1
    ),
    {Collected2, [], _, _} =
        kafine_produce_accumulator:collect_request(TopicPartitions, false, State4),
    ?assertMatch(
        #{
            Topic :=
                #{
                    ?PARTITION_3 := {[{Message3, From3}], _}
                }
        },
        Collected2
    ).

collect_with_gather_prefers_completed_batches() ->
    Topic = ?TOPIC_NAME,

    ProducerOptions0 = ?PRODUCER_OPTIONS,
    ProducerOptions = ProducerOptions0#{
        max_batch_size_bytes := 100,
        % Can only fit two batches in a request
        max_request_size_bytes := 200
    },
    State0 = kafine_produce_accumulator:init(ProducerOptions, #{}),

    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    Message2 = message(#{key => <<"key2">>, value => <<"value2">>}),
    From2 = ?FROM(test2),
    {ok, true, false, State2} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_2,
        Message2,
        From2,
        State1
    ),

    Message3 = message(#{key => <<"key3">>, value => <<"value3">>}),
    From3 = ?FROM(test3),
    {ok, true, false, State3} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_3,
        Message3,
        From3,
        State2
    ),

    % Gather partition 3. The test only_collects_batches_up_to_max_request_size proved that it's the
    % one collected last
    State4 = kafine_produce_accumulator:gather_batch(Topic, ?PARTITION_3, State3),

    TopicPartitions = #{Topic => [?PARTITION_1, ?PARTITION_2, ?PARTITION_3]},
    {Collected1, [{Topic, ?PARTITION_1}], _, State5} =
        kafine_produce_accumulator:collect_request(TopicPartitions, true, State4),
    ?assertMatch(
        #{
            Topic :=
                #{
                    ?PARTITION_3 := {[{Message3, From3}], _},
                    ?PARTITION_1 := {[{Message1, From1}], _}
                }
        },
        Collected1
    ),
    {Collected2, [{Topic, ?PARTITION_2}], _, _} =
        kafine_produce_accumulator:collect_request(TopicPartitions, true, State5),
    ?assertMatch(
        #{
            Topic :=
                #{
                    ?PARTITION_2 := {[{Message2, From2}], _}
                }
        },
        Collected2
    ).

busy_partition_can_not_starve_other_partitions() ->
    Topic = ?TOPIC_NAME,
    Topic2 = ?TOPIC_NAME_2,

    % actually use random numbers for this test
    meck:expect(rand, uniform, fun(N) -> meck:passthrough([N]) end),

    ProducerOptions0 = ?PRODUCER_OPTIONS,
    ProducerOptions = ProducerOptions0#{
        % Always collect batches immediately
        max_batch_size_bytes := 0,
        % Can only fit one batch in a request
        max_request_size_bytes := 100
    },
    State0 = kafine_produce_accumulator:init(ProducerOptions, #{}),

    % Create a single message for a slow partition on each topic
    Message1 = message(#{key => <<"slow1">>, value => <<"slow1">>}),
    From1 = ?FROM(test1),
    {ok, false, true, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_2,
        Message1,
        From1,
        State0
    ),

    Message2 = message(#{key => <<"slow2">>, value => <<"slow2">>}),
    From2 = ?FROM(test1),
    {ok, false, true, State2} = kafine_produce_accumulator:append(
        Topic2,
        ?PARTITION_2,
        Message2,
        From2,
        State1
    ),

    % Include
    TopicPartitions = #{
        Topic => [?PARTITION_1, ?PARTITION_2, ?PARTITION_3],
        Topic2 => [?PARTITION_1, ?PARTITION_2]
    },
    {Got1, Got2, _} =
        lists:foldl(
            fun(_N, {Got1, Got2, StateAcc}) ->
                % produce messages to other partitions and topics
                Message = message(#{key => <<"fast">>, value => <<"fast">>}),
                From = ?FROM(fast),

                {ok, false, true, StateAcc1} = kafine_produce_accumulator:append(
                    Topic,
                    ?PARTITION_1,
                    Message,
                    From,
                    StateAcc
                ),

                {ok, false, true, StateAcc2} = kafine_produce_accumulator:append(
                    Topic,
                    ?PARTITION_3,
                    Message,
                    From,
                    StateAcc1
                ),

                {ok, false, true, StateAcc3} = kafine_produce_accumulator:append(
                    Topic2,
                    ?PARTITION_1,
                    Message,
                    From,
                    StateAcc2
                ),

                %?LOG_ALERT("State: ~p", [StateAcc3]),

                % Collect request, should have a single message
                {Collected, [], _, StateAcc4} =
                    kafine_produce_accumulator:collect_request(TopicPartitions, false, StateAcc3),

                case Collected of
                    #{Topic := #{?PARTITION_2 := {[{Message1, From1}], _}}} ->
                        % Got Message1
                        {true, Got2, StateAcc4};
                    #{Topic2 := #{?PARTITION_2 := {[{Message2, From2}], _}}} ->
                        % Got Message2
                        {Got1, true, StateAcc4};
                    _ ->
                        % Got one of the other messages
                        {Got1, Got2, StateAcc4}
                end
            end,
            {false, false, State2},
            % Should comfortably be enough iterations that this test never realistically fails
            lists:seq(1, 100)
        ),

    ?assertEqual(true, Got1),
    ?assertEqual(true, Got2).

creates_telemetry_events() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, producer, init_batch],
        [kafine, producer, gather_batch],
        [kafine, producer, collect_batch],
        [kafine, producer, requeue_batch]
    ]),

    Topic = ?TOPIC_NAME,
    Topic2 = ?TOPIC_NAME_2,
    Ref = test_ref,

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{ref => Ref}),

    % Append a message
    Message1 = message(#{key => <<"key1">>, value => <<"value1">>}),
    From1 = ?FROM(test1),
    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message1,
        From1,
        State0
    ),

    % First message for a partition should trigger init_batch
    ?assertReceived(
        {[kafine, producer, init_batch], _, #{}, #{
            ref := Ref,
            topic := Topic,
            partition := ?PARTITION_1
        }}
    ),

    % Append another message
    Message2 = message(#{key => <<"key2">>, value => <<"value2">>}),
    From2 = ?FROM(test2),
    {ok, false, false, State2} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_1,
        Message2,
        From2,
        State1
    ),

    % Gather batch
    State3 = kafine_produce_accumulator:gather_batch(Topic, ?PARTITION_1, State2),

    % Should trigger gather batch stop telemetry
    ?assertReceived(
        {
            [kafine, producer, gather_batch],
            _,
            #{
                linger_us := _,
                uncompressed_size_bytes := 95,
                length := 2,
                queue_length := 1
            },
            #{
                ref := Ref,
                topic := Topic,
                partition := ?PARTITION_1
            }
        }
    ),

    % Add messages for 2 other partitions (one on a different topic)
    Message3 = message(#{key => <<"key3">>, value => <<"value3">>}),
    From3 = ?FROM(test3),
    {ok, true, false, State4} = kafine_produce_accumulator:append(
        Topic,
        ?PARTITION_2,
        Message3,
        From3,
        State3
    ),

    Message4 = message(#{key => <<"key4">>, value => <<"value4">>}),
    From4 = ?FROM(test4),
    {ok, true, false, State5} = kafine_produce_accumulator:append(
        Topic2,
        ?PARTITION_1,
        Message4,
        From4,
        State4
    ),

    % Collect a request
    TopicPartitions = #{
        Topic => [?PARTITION_1, ?PARTITION_2],
        Topic2 => [?PARTITION_1]
    },
    {Batches, _, Stats, State6} = kafine_produce_accumulator:collect_request(
        TopicPartitions, true, State5
    ),

    % Should trigger batch queue stop telemetry
    ?assertReceived(
        {
            [kafine, producer, collect_batch],
            _,
            #{
                queue_length := 0,
                queue_time_us := _,
                batch_age_us := _
            },
            #{
                ref := Ref,
                topic := Topic,
                partition := ?PARTITION_1
            }
        }
    ),

    % Stats aren't telemetry issued by the accumulator, but they are directly used for that by the
    % producer, so we test them in the metrics test
    ?assertEqual(
        #{
            uncompressed_size_bytes => 251,
            message_count => 4,
            partition_count => 3
        },
        Stats
    ),

    _ = kafine_produce_accumulator:requeue_batches(Batches, State6),

    ?assertReceived(
        {
            [kafine, producer, requeue_batch],
            _,
            #{
                uncompressed_size_bytes := 95,
                length := 2,
                queue_length := 1,
                batch_age_us := _
            },
            #{
                ref := Ref,
                topic := Topic,
                partition := ?PARTITION_1
            }
        }
    ).

info() ->
    Topic = ?TOPIC_NAME,
    Message = message(#{key => <<"key1">>, value => <<"value1">>}),
    From = ?FROM(test1),

    State0 = kafine_produce_accumulator:init(?PRODUCER_OPTIONS, #{}),

    ?assertEqual(#{}, kafine_produce_accumulator:info(State0)),

    {ok, true, false, State1} = kafine_produce_accumulator:append(
        Topic, ?PARTITION_1, Message, From, State0
    ),

    ?assertMatch(
        #{
            Topic := #{
                ?PARTITION_1 := #{
                    current_batch := #{message_count := 1, batch_size_bytes := 78},
                    queued_batches := []
                }
            }
        },
        kafine_produce_accumulator:info(State1)
    ),

    State2 = kafine_produce_accumulator:gather_batch(Topic, ?PARTITION_1, State1),

    ?assertMatch(
        #{
            Topic := #{
                ?PARTITION_1 := #{
                    current_batch := #{message_count := 0, batch_size_bytes := 61},
                    queued_batches := [#{message_count := 1, batch_size_bytes := 78}]
                }
            }
        },
        kafine_produce_accumulator:info(State2)
    ).

fair_reduce_while_from_first() ->
    meck:expect(rand, uniform, fun(5) -> 1 end),
    List = [1, 2, 3, 4, 5],
    {cont, Result} =
        kafine_produce_accumulator:fair_reduce_while(
            fun(X, Acc) -> {cont, [X | Acc]} end,
            [],
            List
        ),
    ?assertEqual([1, 2, 3, 4, 5], lists:reverse(Result)).

fair_reduce_while_from_middle() ->
    meck:expect(rand, uniform, fun(5) -> 3 end),
    List = [1, 2, 3, 4, 5],
    {cont, Result} =
        kafine_produce_accumulator:fair_reduce_while(
            fun(X, Acc) -> {cont, [X | Acc]} end,
            [],
            List
        ),
    ?assertEqual([3, 4, 5, 1, 2], lists:reverse(Result)).

fair_reduce_while_from_last() ->
    meck:expect(rand, uniform, fun(5) -> 5 end),
    List = [1, 2, 3, 4, 5],
    {cont, Result} =
        kafine_produce_accumulator:fair_reduce_while(
            fun(X, Acc) -> {cont, [X | Acc]} end,
            [],
            List
        ),
    ?assertEqual([5, 1, 2, 3, 4], lists:reverse(Result)).

message(Fields) ->
    Defaults = #{
        key => null,
        value => null,
        headers => [],
        timestamp => ?TIMESTAMP
    },
    maps:merge(Defaults, Fields).
