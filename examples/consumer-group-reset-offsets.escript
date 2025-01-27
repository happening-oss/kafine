#!/usr/bin/env escript

% Example of using kafine to reset consumer group offsets.
%
% Roughly equivalent to the following:
%   ./bin/kafka-consumer-groups.sh --bootstrap-server SERVER --group GROUP --topic TOPIC --reset-offsets --to-latest --execute
%
% Run it with:
%   ERL_LIBS=$(rebar3 path --lib) ./examples/consumer-group-reset-offsets.escript SERVER GROUP TOPIC

-mode(compile).
-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/coordinator_type.hrl").
-include_lib("kafcod/include/isolation_level.hrl").
-include_lib("kafcod/include/timestamp.hrl").
-include_lib("kafcod/include/uuid.hrl").

main(Args) ->
    argparse:run(
        Args,
        #{
            arguments => [
                #{
                    name => broker,
                    help => "Bootstrap broker (host[:port])",
                    type => {custom, fun parse_broker/1}
                },
                #{
                    name => group,
                    type => binary
                },
                #{
                    name => topic,
                    type => binary
                }
            ],
            handler => fun reset_offsets/1
        },
        #{progname => delete_topics}
    ).

-define(DEFAULT_BROKER_PORT, 9092).

parse_broker(Arg) when is_list(Arg) ->
    case string:split(Arg, ":") of
        [Host, Port] ->
            #{host => Host, port => list_to_integer(Port)};
        [Host] ->
            #{host => Host, port => ?DEFAULT_BROKER_PORT};
        _ ->
            error(badarg)
    end.

reset_offsets(#{broker := BootstrapServer, group := GroupId, topic := TopicName}) ->
    _ = application:ensure_all_started([kafine]),

    % Find the coordinator for the group.
    {ok, Bo} = kafine_connection:start_link(BootstrapServer, #{}),
    FindCoordinatorRequest = #{
        key_type => ?COORDINATOR_TYPE_GROUP,
        coordinator_keys => [GroupId]
    },
    {ok, FindCoordinatorResponse} = kafine_connection:call(
        Bo,
        fun find_coordinator_request:encode_find_coordinator_request_4/1,
        FindCoordinatorRequest,
        fun find_coordinator_response:decode_find_coordinator_response_4/1
    ),
    kafine_connection:stop(Bo),
    #{coordinators := [Coordinator]} = FindCoordinatorResponse,
    #{error_code := ?NONE} = Coordinator,

    % Connect to the coordinator.
    {ok, Co} = kafine_connection:start_link(maps:with([host, port, node_id], Coordinator), #{}),

    % Assert that there are no members in the group.
    DescribeGroupsRequest = #{
        groups => [GroupId],
        include_authorized_operations => false
    },
    {ok, DescribeGroupsResponse} = kafine_connection:call(
        Co,
        fun describe_groups_request:encode_describe_groups_request_5/1,
        DescribeGroupsRequest,
        fun describe_groups_response:decode_describe_groups_response_5/1
    ),
    #{groups := [#{error_code := ?NONE, group_state := <<"Empty">>}]} = DescribeGroupsResponse,

    % Get the offsets for the topic; this requires talking to the leader of each partition. For that, we need the
    % metadata.
    MetadataRequest = #{
        topics => [
            #{
                name => TopicName,
                topic_id => ?NULL_UUID
            }
        ],
        allow_auto_topic_creation => false,
        include_topic_authorized_operations => false
    },
    {ok, MetadataResponse} = kafine_connection:call(
        Co,
        fun metadata_request:encode_metadata_request_12/1,
        MetadataRequest,
        fun metadata_response:decode_metadata_response_12/1
    ),
    #{brokers := Brokers, topics := TopicsMetadata} = MetadataResponse,

    % Group the partitions by leader ID; saves a few round-trips.
    % Note that this is an internal function; it's not part of the stable API.
    PartitionsByLeaderId = kafine_metadata:group_by_leader(TopicsMetadata),

    % For each leader, ask it about the offsets.
    LatestOffsets = maps:fold(
        fun(LeaderId, Topics, Offsets) ->
            [Leader] = [B || B = #{node_id := NodeId} <- Brokers, NodeId == LeaderId],

            % Build a ListOffsets request; note that the following bit deals with multiple topics, even though we only
            % originally asked about one.
            ListOffsetsTopics = maps:fold(
                fun(Topic, Partitions, Acc) ->
                    [
                        #{
                            name => Topic,
                            partitions => [
                                #{
                                    partition_index => PartitionIndex,
                                    current_leader_epoch => -1,
                                    timestamp => ?LATEST_TIMESTAMP
                                }
                             || PartitionIndex <- Partitions
                            ]
                        }
                        | Acc
                    ]
                end,
                [],
                Topics
            ),
            ListOffsetsRequest = #{
                topics => ListOffsetsTopics,
                replica_id => -1,
                isolation_level => ?READ_UNCOMMITTED
            },
            {ok, Le} = kafine_connection:start_link(Leader, #{}),
            {ok, ListOffsetsResponse} = kafine_connection:call(
                Le,
                fun list_offsets_request:encode_list_offsets_request_8/1,
                ListOffsetsRequest,
                fun list_offsets_response:decode_list_offsets_response_8/1
            ),
            kafine_connection:stop(Le),

            % Unlike the above, this expects a single topic.
            #{topics := ListOffsetsTopicResponses} = ListOffsetsResponse,
            [#{name := TopicName, partitions := ListOffsetsPartitionResponses}] =
                ListOffsetsTopicResponses,
            lists:foldl(
                fun(#{error_code := ?NONE, partition_index := P, offset := LatestOffset}, Acc) ->
                    Acc#{P => LatestOffset}
                end,
                Offsets,
                ListOffsetsPartitionResponses
            )
        end,
        #{},
        PartitionsByLeaderId
    ),

    OffsetCommitRequest = #{
        group_id => GroupId,
        generation_id_or_member_epoch => -1,
        group_instance_id => null,
        member_id => <<>>,
        topics => [
            #{
                name => TopicName,
                partitions => maps:fold(
                    fun(PartitionIndex, Offset, Acc) ->
                        CommittedOffset = Offset,
                        CommittedMetadata = null,
                        [
                            #{
                                partition_index => PartitionIndex,
                                committed_offset => CommittedOffset,
                                committed_metadata => CommittedMetadata,
                                committed_leader_epoch => -1
                            }
                            | Acc
                        ]
                    end,
                    [],
                    LatestOffsets
                )
            }
        ]
    },
    {ok, OffsetCommitResponse} = kafine_connection:call(
        Co,
        fun offset_commit_request:encode_offset_commit_request_8/1,
        OffsetCommitRequest,
        fun offset_commit_response:decode_offset_commit_response_8/1
    ),
    io:format("~p~n", [OffsetCommitResponse]),
    kafine_connection:stop(Co),
    ok.
