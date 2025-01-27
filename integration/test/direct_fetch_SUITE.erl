-module(direct_fetch_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/isolation_level.hrl").

all() ->
    [
        direct_fetch_single_partition_empty,
        direct_fetch_single_partition_single_message
    ].

suite() ->
    [
        {require, bootstrap_server}
    ].

init_per_testcase(_TestCase, Config) ->
    Config.

% TODO: There's a lot to be said for *not* cleaning up afterwards -- it's easier to debug.
% Obviously, you need to clean up first if you're doing that.
end_per_testcase(TestCase, _Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),
    TopicName = iolist_to_binary(io_lib:format("~s_~s", [?MODULE, TestCase])),
    ok = kafka_fixtures:delete_topic(Bootstrap, TopicName),
    ok.

-define(CLIENT_ID, atom_to_binary(?MODULE)).

parse_broker(Broker) when is_list(Broker) ->
    [Host, Port] = string:split(Broker, ":"),
    #{host => list_to_binary(Host), port => list_to_integer(Port)}.

direct_fetch_single_partition_empty(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = iolist_to_binary(io_lib:format("~s_~s", [?MODULE, ?FUNCTION_NAME])),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    PartitionIndex = 0,

    % Find the leader for the partition.
    Leader = kafka_fixtures:get_leader(Bootstrap, TopicName, PartitionIndex),
    {ok, C} = kafine_connection:start_link(Leader, #{client_id => ?CLIENT_ID}),

    FetchOffset = 0,

    % The test will take about this long to run.
    MaxWaitMs = 1_000,
    % This number's kinda small; doesn't matter for an empty partition.
    MaxBytes = 1_024,

    FetchRequest = build_fetch_request(
        TopicName,
        PartitionIndex,
        FetchOffset,
        _Options = #{max_wait_ms => MaxWaitMs, max_bytes => MaxBytes}
    ),

    {ok, #{
        error_code := ?NONE,
        responses := [
            #{
                topic := TopicName,
                partitions := [
                    #{partition_index := PartitionIndex, error_code := ?NONE, records := Records}
                ]
            }
        ]
    }} = kafine_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),
    [] = Records,
    ok.

direct_fetch_single_partition_single_message(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    TopicName = iolist_to_binary(io_lib:format("~s_~s", [?MODULE, ?FUNCTION_NAME])),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    PartitionIndex = 0,

    Key = iolist_to_binary(
        io_lib:format("~s:~s:~B", [?MODULE, ?FUNCTION_NAME, erlang:system_time()])
    ),
    MessageLength = 180,
    Value = base64:encode(rand:bytes(MessageLength)),
    Message = #{key => Key, value => Value, headers => []},
    ok = kafka_fixtures:produce_message(Bootstrap, TopicName, PartitionIndex, Message),

    Leader = kafka_fixtures:get_leader(Bootstrap, TopicName, PartitionIndex),
    {ok, C} = kafine_connection:start_link(Leader, #{client_id => ?CLIENT_ID}),

    FetchOffset = 0,

    % The test will take about this long to run.
    MaxWaitMs = 1_000,
    % This number's kinda small; doesn't matter for an empty partition.
    MaxBytes = 1_024,

    FetchRequest = build_fetch_request(
        TopicName,
        PartitionIndex,
        FetchOffset,
        _Options = #{max_wait_ms => MaxWaitMs, max_bytes => MaxBytes}
    ),

    {ok, #{
        error_code := ?NONE,
        responses := [
            #{
                topic := TopicName,
                partitions := [
                    #{partition_index := PartitionIndex, error_code := ?NONE, records := Records}
                ]
            }
        ]
    }} = kafine_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    % TODO: Loop until the message appears.
    [_] = Records,
    ok.

build_fetch_request(
    TopicName,
    PartitionIndex,
    FetchOffset,
    _Options = #{max_wait_ms := MaxWaitMs, max_bytes := MaxBytes}
) ->
    #{
        replica_id => -1,
        max_wait_ms => MaxWaitMs,
        min_bytes => 1,
        max_bytes => MaxBytes,
        isolation_level => ?READ_COMMITTED,
        topics => [
            #{
                topic => TopicName,
                partitions => [
                    #{
                        partition => PartitionIndex,
                        fetch_offset => FetchOffset,
                        partition_max_bytes => MaxBytes,
                        log_start_offset => -1,
                        current_leader_epoch => -1
                    }
                ]
            }
        ],
        session_id => 0,
        session_epoch => -1,
        rack_id => <<"undefined">>,
        forgotten_topics_data => []
    }.
