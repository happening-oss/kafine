-module(direct_create_topic_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("kafcod/include/error_code.hrl").

all() ->
    [
        create_topics_with_assignments
    ].

suite() ->
    [
        {require, bootstrap_server}
    ].

-define(CLIENT_ID, atom_to_binary(?MODULE)).
-define(TIMEOUT_MS, 5_000).

-define(make_topic_name(),
    iolist_to_binary(
        io_lib:format("~s_~s_~s", [?MODULE, ?FUNCTION_NAME, base64url:encode(rand:bytes(6))])
    )
).

parse_broker(Broker) when is_list(Broker) ->
    [Host, Port] = string:split(Broker, ":"),
    #{host => list_to_binary(Host), port => list_to_integer(Port)}.

create_topics_with_assignments(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),

    {ok, C} = kafine_connection:start_link(Bootstrap, #{client_id => ?CLIENT_ID}),
    {ok, #{controller_id := ControllerId, brokers := Brokers}} = kafine_connection:call(
        C,
        fun metadata_request:encode_metadata_request_9/1,
        #{
            topics => [],
            allow_auto_topic_creation => false,
            include_cluster_authorized_operations => false,
            include_topic_authorized_operations => false
        },
        fun metadata_response:decode_metadata_response_9/1
    ),

    Controller = get_node_by_id(Brokers, ControllerId),
    {ok, Co} = kafine_connection:start_link(Controller, #{client_id => ?CLIENT_ID}),

    TopicName = ?make_topic_name(),
    NumPartitions = 10,
    ReplicationFactor = 2,

    % Assign the partitions to the first 2 brokers in the list.
    AssignedBrokerIds = lists:sublist(
        lists:sort([NodeId || #{node_id := NodeId} <- Brokers]), ReplicationFactor
    ),
    Assignments = [
        #{partition_index => PartitionIndex, broker_ids => shuffle(AssignedBrokerIds)}
     || PartitionIndex <- lists:seq(0, NumPartitions - 1)
    ],

    {ok, #{topics := [#{error_code := ?NONE}]}} = kafine_connection:call(
        Co,
        fun create_topics_request:encode_create_topics_request_5/1,
        #{
            topics => [
                #{
                    name => TopicName,

                    % Tell the cluster where we want the partitions placed.
                    assignments => Assignments,

                    % Because we're explicitly setting 'assignments', we need to NOT set these.
                    num_partitions => -1,
                    replication_factor => -1,

                    % Defaults.
                    configs => []
                }
            ],
            timeout_ms => ?TIMEOUT_MS,
            validate_only => false
        },
        fun create_topics_response:decode_create_topics_response_5/1
    ),

    % TODO: Actually assert that the topic was created and placed where we asked. For now, manual inspection's fine,
    % though.

    ok.

get_node_by_id(Nodes, NodeId) when is_list(Nodes), is_integer(NodeId) ->
    [Node] = [N || N = #{node_id := Id} <- Nodes, Id =:= NodeId],
    Node.

shuffle(List) when is_list(List) ->
    % From https://stackoverflow.com/a/8820501
    [X || {_, X} <- lists:sort([{rand:uniform(), N} || N <- List])].
