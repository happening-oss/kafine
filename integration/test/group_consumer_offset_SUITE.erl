-module(group_consumer_offset_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

all() ->
    [
        resume_offset
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
-define(FETCHER_METADATA, #{}).

resume_offset(_Config) ->
    TelemetryRef = telemetry_test:attach_event_handlers(self(), [
        [kafine, rebalance, leader]
    ]),
    % This test checks that we can resume from a committed offset. We produce 20 messages, then commit offset 10.
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    % Create a topic.
    TopicName = ?make_topic_name(),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),
    PartitionIndex = 0,

    % Produce a bunch of messages.
    InitialCount = 20,
    _Keys = [
        produce_message(Bootstrap, TopicName, PartitionIndex)
     || _ <- lists:seq(1, InitialCount)
    ],

    % Start membership
    GroupId = ?make_group_name(),
    CommittedOffset = 10,
    MembershipRef = setup_committed_offsets,
    MembershipOptions = kafine_membership_options:validate_options(#{
        subscription_callback => {do_nothing_subscription_callback, []},
        assignment_callback => {do_nothing_assignment_callback, []}
    }),
    {ok, B} = kafine_bootstrap:start_link(MembershipRef, Bootstrap, #{}),
    {ok, M} = kafine_metadata_cache:start_link(MembershipRef),
    {ok, C} = kafine_coordinator:start_link(
        MembershipRef, GroupId, [TopicName], #{}, MembershipOptions
    ),
    {ok, R} = kafine_eager_rebalance:start_link(MembershipRef, [TopicName], GroupId, MembershipOptions),

    % Wait for leader assignment so that we know we found the coordinator
    receive
        {[kafine, rebalance, leader], TelemetryRef, #{}, #{group_id := GroupId}} -> ok
    end,

    % Commit offset
    Offsets = #{TopicName => #{PartitionIndex => CommittedOffset}},
    {
        ok,
        #{
            TopicName := #{
                PartitionIndex := ok
            }
        },
        0
    } = kafine_eager_rebalance:offset_commit(MembershipRef, Offsets),

    kafine_eager_rebalance:stop(R),
    kafine_coordinator:stop(C),
    kafine_metadata_cache:stop(M),
    kafine_bootstrap:stop(B),

    % TODO? Assert the consumer lag while we've got some?
    ConsumerRef = ?CONSUMER_REF,
    % Then we can get on with the test.
    {ok, _C} = kafine:start_group_consumer(
        ConsumerRef,
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

    ExpectedFirstOffset = 10,
    ExpectedCount = 10,
    eventually:assert(
        records_received(),
        % The lowest offset should be the expected first offset, and we should see at least the expected number of
        % records.
        eventually:match(fun(Records = [{_Topic, _Partition, #{offset := Offset}} | _]) ->
            Offset == ExpectedFirstOffset andalso length(Records) >= ExpectedCount
        end)
    ),
    kafine:stop_group_consumer(ConsumerRef),
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
