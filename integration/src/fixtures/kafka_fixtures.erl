-module(kafka_fixtures).
-export([
    create_topic/2,
    delete_topic/2,
    get_leader/3,
    produce_message/4,
    delete_records/5
]).
-include_lib("kafcod/include/error_code.hrl").

-define(CLIENT_ID, <<"kafine_tests">>).
-define(TIMEOUT_MS, 5_000).

% TODO: rename to 'ensure_topic_exists'?
create_topic(Broker, TopicName) ->
    create_topic(Broker, TopicName, 10, 3).

create_topic(Broker, TopicName, NumPartitions, ReplicationFactor) ->
    % Does the topic exist? Use a metadata call to find out (or, if not, to get the controller ID).
    {ok, C} = kafine_connection:start_link(Broker, #{client_id => ?CLIENT_ID}),
    case
        kafine_connection:call(
            C,
            fun metadata_request:encode_metadata_request_9/1,
            #{
                topics => [#{name => TopicName}],
                allow_auto_topic_creation => false,
                include_cluster_authorized_operations => false,
                include_topic_authorized_operations => false
            },
            fun metadata_response:decode_metadata_response_9/1
        )
    of
        {ok, #{topics := [#{error_code := ?NONE}]}} ->
            % Topic already exists.
            ok;
        {ok, #{
            topics := [#{error_code := ?UNKNOWN_TOPIC_OR_PARTITION}],
            controller_id := ControllerId,
            brokers := Brokers
        }} ->
            % Topic does not exist; find the controller and then create it.
            Controller = get_node_by_id(Brokers, ControllerId),

            % Connect to the controller.
            {ok, Co} = kafine_connection:start_link(Controller, #{client_id => ?CLIENT_ID}),

            {ok, #{topics := [#{error_code := ?NONE}]}} = kafine_connection:call(
                Co,
                fun create_topics_request:encode_create_topics_request_2/1,
                #{
                    topics => [
                        #{
                            name => TopicName,
                            num_partitions => NumPartitions,
                            replication_factor => ReplicationFactor,

                            % Use the defaults.
                            configs => [],
                            assignments => []
                        }
                    ],
                    timeout_ms => ?TIMEOUT_MS,
                    validate_only => false
                },
                fun create_topics_response:decode_create_topics_response_2/1
            ),

            eventually:assert(kafka_topic_assert:topic_present_on_all_brokers(TopicName, Brokers)),
            ok
    end.

delete_topic(Broker, TopicName) when is_binary(TopicName) ->
    {ok, C} = kafine_connection:start_link(Broker, #{client_id => ?CLIENT_ID}),

    % Get the controller.
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
    {ok, #{
        responses := [
            #{
                name := TopicName,
                error_code := ?NONE
            }
        ]
    }} = kafine_connection:call(
        Co,
        % Wireshark supports up to v4.
        fun delete_topics_request:encode_delete_topics_request_4/1,
        #{
            topic_names => [TopicName],
            timeout_ms => ?TIMEOUT_MS
        },
        fun delete_topics_response:decode_delete_topics_response_4/1
    ),
    ok.

get_leader(Broker, TopicName, PartitionIndex) when is_map(Broker) ->
    % TODO: Invent 'using_connection' function.
    {ok, C} = kafine_connection:start_link(Broker, #{client_id => ?CLIENT_ID}),
    Leader = get_leader(C, TopicName, PartitionIndex),
    kafine_connection:stop(C),
    Leader;
get_leader(Connection, TopicName, PartitionIndex) when is_pid(Connection) ->
    {ok, #{topics := [#{error_code := ?NONE, partitions := Partitions}], brokers := Brokers}} = kafine_connection:call(
        Connection,
        fun metadata_request:encode_metadata_request_9/1,
        #{
            topics => [#{name => TopicName}],
            allow_auto_topic_creation => false,
            include_cluster_authorized_operations => false,
            include_topic_authorized_operations => false
        },
        fun metadata_response:decode_metadata_response_9/1
    ),

    [LeaderId] = [
        LeaderId
     || #{partition_index := P, error_code := ?NONE, leader_id := LeaderId} <- Partitions,
        P =:= PartitionIndex
    ],

    [Leader] = [Broker || Broker = #{node_id := Id} <- Brokers, Id =:= LeaderId],
    Leader.

% Philosophical note: there is duplication (here in the fixture, and the test in direct_produce_SUITE). That's the kind
% of thing that happens when you've got a bunch of tests at different levels of abstraction: necessarily some of the
% higher-level tests will rely on fixtures that duplicate lower-level tests.
%
% And that's fine.
%
% DRY is all about having two copies of something that would need to change at the same time, in response to the same
% stimulus.
%
% That's not -- quite -- what we've got here. For example: if we decide we want to add a bunch of tests that check
% different permutations (or versions) of the Produce request, that impacts those tests specifically. It doesn't impact
% the other tests that depend on this fixture.
%
% Or, to look at it another way: coupling tests that don't particularly care about Produce to the tests specifically for
% Produce is a bad idea.
produce_message(Broker, TopicName, PartitionIndex, Message) ->
    Records = kafcod_message_set:prepare_message_set(#{compression => none}, [Message]),
    Leader = get_leader(Broker, TopicName, PartitionIndex),
    {ok, C} = kafine_connection:start_link(Leader, #{client_id => ?CLIENT_ID}),

    {ok, #{
        responses := [
            #{
                partition_responses := [
                    #{index := PartitionIndex, error_code := ?NONE}
                ]
            }
        ]
    }} = kafine_connection:call(
        C,
        % Wireshark supports v8; it makes life easier to stick to that, unless we're specifically testing newer
        % versions.
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
    ok.

delete_records(Broker, TopicName, PartitionIndex, DeleteFrom, DeleteTo) ->
    Leader = get_leader(Broker, TopicName, PartitionIndex),
    {ok, L} = kafine_connection:start_link(Leader, #{client_id => ?CLIENT_ID}),
    {ok, #{
        topics := [
            #{
                name := TopicName,
                partitions := [#{partition_index := PartitionIndex, error_code := ?NONE}]
            }
        ]
    }} = kafine_connection:call(
        L,
        fun delete_records_request:encode_delete_records_request_1/1,
        #{
            timeout_ms => ?TIMEOUT_MS,
            topics => [
                #{
                    name => TopicName,

                    % We can delete multiple records by putting multiple entries in this array with the same partition
                    % index, but different offsets.
                    partitions => [
                        #{
                            partition_index => PartitionIndex,
                            offset => Offset
                        }
                     || Offset <- lists:seq(DeleteFrom, DeleteTo)
                    ]
                }
            ]
        },
        fun delete_records_response:decode_delete_records_response_1/1
    ),
    ok.

get_node_by_id(Nodes, NodeId) when is_list(Nodes), is_integer(NodeId) ->
    [Node] = [N || N = #{node_id := Id} <- Nodes, Id =:= NodeId],
    Node.
