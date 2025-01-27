-module(kafka_topic_assert).
-export([
    topic_present_on_all_brokers/2,
    get_message_count/3,
    get_offset_of_topic_partition/4
]).
-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/timestamp.hrl").
-include_lib("kafcod/include/isolation_level.hrl").

-define(CLIENT_ID, atom_to_binary(?MODULE)).

topic_present_on_all_brokers(TopicName, Brokers) ->
    fun() ->
        lists:all(
            fun(Broker) ->
                {ok, C} = kafine_connection:start_link(Broker, #{client_id => ?CLIENT_ID}),
                {ok, #{topics := [#{name := TopicName, error_code := ?NONE}]}} = kafine_connection:call(
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
                true
            end,
            Brokers
        )
    end.

get_message_count(Connection, TopicName, PartitionIndex) ->
    EarliestOffset = get_offset_of_topic_partition(
        Connection, TopicName, PartitionIndex, ?EARLIEST_TIMESTAMP
    ),
    LatestOffset = get_offset_of_topic_partition(
        Connection, TopicName, PartitionIndex, ?LATEST_TIMESTAMP
    ),

    LatestOffset - EarliestOffset.

get_offset_of_topic_partition(Connection, TopicName, PartitionIndex, Timestamp) ->
    {ok, #{
        topics := [
            #{
                name := TopicName,
                partitions := [
                    #{
                        partition_index := PartitionIndex,
                        offset := Offset,
                        error_code := ?NONE
                    }
                ]
            }
        ]
    }} = kafine_connection:call(
        Connection,
        % v5 is the latest that Wireshark supports, so we'll use that.
        fun list_offsets_request:encode_list_offsets_request_5/1,
        #{
            topics => [
                #{
                    name => TopicName,
                    partitions => [
                        #{
                            partition_index => PartitionIndex,
                            current_leader_epoch => -1,
                            timestamp => Timestamp
                        }
                    ]
                }
            ],
            isolation_level => ?READ_COMMITTED,
            % We're a consumer, rather than a broker, so -1.
            replica_id => -1
        },
        fun list_offsets_response:decode_list_offsets_response_5/1
    ),
    Offset.
