-module(kafine_group_consumer_subscription_callback).
-behaviour(kafine_subscription_callback).
-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/api_key.hrl").

-export([
    init/1,
    subscribe_partitions/3,
    unsubscribe_partitions/1
]).

-record(state, {
    consumer :: pid(),
    group_id :: binary(),
    topic_options :: kafine:topic_options(),
    offset_callback :: module()
}).

init([Consumer, GroupId, Topics, TopicOptions, OffsetCallback]) ->
    ok = kafine_behaviour:verify_callbacks_exported(
        kafine_adjust_fetched_offset_callback, OffsetCallback
    ),
    {ok, #state{
        consumer = Consumer,
        group_id = GroupId,
        topic_options = kafine_topic_options:validate_options(Topics, TopicOptions),
        offset_callback = OffsetCallback
    }}.

subscribe_partitions(
    Coordinator,
    AssignedPartitions,
    State = #state{
        consumer = Consumer,
        group_id = GroupId,
        topic_options = TopicOptions,
        offset_callback = OffsetCallback
    }
) ->
    % Get the initial committed offsets.
    {ok, #{error_code := ?NONE, topics := TopicPartitionOffsets}} = offset_fetch(
        Coordinator,
        GroupId,
        transform_assignment(AssignedPartitions)
    ),
    % Subscribe
    kafine_consumer:subscribe(
        Consumer, make_subscription(TopicPartitionOffsets, TopicOptions, OffsetCallback)
    ),
    {ok, State}.

unsubscribe_partitions(State = #state{consumer = Consumer}) ->
    kafine_consumer:unsubscribe_all(Consumer),
    {ok, State}.

offset_fetch(Connection, GroupId, Topics) when
    is_pid(Connection), is_binary(GroupId), is_list(Topics)
->
    kafine_connection:call(
        Connection,
        fun offset_fetch_request:encode_offset_fetch_request_4/1,
        #{
            group_id => GroupId,
            topics => Topics
        },
        fun offset_fetch_response:decode_offset_fetch_response_4/1,
        kafine_request_telemetry:request_labels(?OFFSET_FETCH, 4, GroupId)
    ).

make_subscription(TopicPartitionOffsets, TopicOptions, OffsetCallback) ->
    lists:foldl(
        fun(#{name := TopicName, partitions := PartitionOffsets}, Acc) ->
            Options = maps:get(TopicName, TopicOptions, #{}),
            #{initial_offset := InitialOffset} = Options,
            PartitionIndexOffset = lists:foldl(
                fun
                    (
                        #{partition_index := PartitionIndex, committed_offset := CommittedOffset},
                        Acc1
                    ) when CommittedOffset >= 0 ->
                        AdjustedCommitOffset = OffsetCallback:adjust_committed_offset(
                            CommittedOffset
                        ),
                        Acc1#{PartitionIndex => AdjustedCommitOffset};
                    (
                        #{partition_index := PartitionIndex, committed_offset := -1},
                        Acc1
                    ) ->
                        Acc1#{PartitionIndex => InitialOffset}
                end,
                #{},
                PartitionOffsets
            ),
            Acc#{TopicName => {Options, PartitionIndexOffset}}
        end,
        #{},
        TopicPartitionOffsets
    ).

transform_assignment(AssignedPartitions) ->
    maps:fold(
        fun(Topic, PartitionIndexes, Acc) ->
            [#{name => Topic, partition_indexes => PartitionIndexes} | Acc]
        end,
        [],
        AssignedPartitions
    ).
