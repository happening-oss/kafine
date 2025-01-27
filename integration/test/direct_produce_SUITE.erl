-module(direct_produce_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(TIMEOUT_MS, 10_000).

all() ->
    [
        direct_produce_one
    ].

suite() ->
    [
        {require, bootstrap_server}
    ].

-define(CLIENT_ID, atom_to_binary(?MODULE)).

-define(make_topic_name(),
    iolist_to_binary(
        io_lib:format("~s_~s_~s", [?MODULE, ?FUNCTION_NAME, base64url:encode(rand:bytes(6))])
    )
).

parse_broker(Broker) when is_list(Broker) ->
    [Host, Port] = string:split(Broker, ":"),
    #{host => list_to_binary(Host), port => list_to_integer(Port)}.

%% The point of kafine is that you can use it at whatever level of abstraction you want. You want to produce a message
%% without worrying about which broker it should go to? That's in there somewhere. You want to be explicit about every
%% detail? We've got that too.
%%
%% Since the first is implemented in terms of the second, it makes some sort of sense to have a test at the lower level
%% of abstraction. This is one of those tests.
direct_produce_one(_Config) ->
    % Connect to the bootstrap server.
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),
    {ok, C} = kafine_connection:start_link(Bootstrap, #{client_id => ?CLIENT_ID}),

    % Note: we're not using Arrange/Act/Assert properly here. We *ought* to be creating the topic as part of environment
    % bring-up, or in 'init_per_suite' or 'init_per_testcase' (depending on the desired granularity). We should also
    % delete the topic in 'end_per_testcase' or 'end_per_suite'.

    % We're gonna need a topic.
    TopicName = ?make_topic_name(),
    ok = kafka_fixtures:create_topic(Bootstrap, TopicName),

    % Find the leader for the topic/partition that we'd like to produce to. Yes, there's some duplication between this
    % and kafka_fixtures.erl; that's fine -- see the philosophical note in that module.
    {ok, #{topics := [#{error_code := ?NONE, partitions := Partitions}], brokers := Brokers}} = kafine_connection:call(
        C,
        fun metadata_request:encode_metadata_request_9/1,
        #{
            topics => [#{name => TopicName}],
            allow_auto_topic_creation => false,
            include_cluster_authorized_operations => false,
            include_topic_authorized_operations => false
        },
        fun metadata_response:decode_metadata_response_9/1
    ),

    PartitionIndex = 0,

    [LeaderId] = [
        LeaderId
     || #{partition_index := P, error_code := ?NONE, leader_id := LeaderId} <- Partitions,
        P =:= PartitionIndex
    ],

    Leader = get_node_by_id(Brokers, LeaderId),
    {ok, L} = kafine_connection:start_link(Leader, #{client_id => ?CLIENT_ID}),

    MessageCount0 = kafka_topic_assert:get_message_count(L, TopicName, PartitionIndex),

    % Produce a message to the leader.
    Records = kafcod_message_set:prepare_message_set(#{compression => none}, [
        #{
            key => <<"key">>,
            value => <<"value">>,
            headers => []
        }
    ]),

    {ok, #{
        responses := [
            #{
                partition_responses := [
                    #{index := PartitionIndex, error_code := ?NONE}
                ]
            }
        ]
    }} = kafine_connection:call(
        L,
        fun produce_request:encode_produce_request_8/1,
        #{
            topic_data => [
                #{
                    name => TopicName,
                    partition_data => [
                        #{
                            index => PartitionIndex,
                            records => Records
                        }
                    ]
                }
            ],
            acks => -1,
            timeout_ms => ?TIMEOUT_MS,
            transactional_id => null
        },
        fun produce_response:decode_produce_response_8/1
    ),

    MessageCount = kafka_topic_assert:get_message_count(L, TopicName, PartitionIndex),
    ?assertEqual(1, MessageCount - MessageCount0),
    ok.

get_node_by_id(Nodes, NodeId) when is_list(Nodes), is_integer(NodeId) ->
    [Node] = [N || N = #{node_id := Id} <- Nodes, Id =:= NodeId],
    Node.
