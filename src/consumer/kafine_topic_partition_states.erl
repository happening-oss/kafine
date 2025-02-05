-module(kafine_topic_partition_states).
-export([
    merge_topic_partition_states/2,
    take_partition_states/2
]).

merge_topic_partition_states(TopicPartitionStates0, TopicPartitionStates1) ->
    maps:merge_with(fun combine_partition_states/3, TopicPartitionStates0, TopicPartitionStates1).

% Suppress "Function ... only terminates with explicit exception" and "The created fun has no local return".
% Because that's entirely the point.

-dialyzer({nowarn_function, combine_partition_states/3}).

combine_partition_states(TopicName, PartitionStates0, PartitionStates1) ->
    maps:merge_with(
        fun(PartitionIndex, _, _) ->
            error({already_subscribed, TopicName, PartitionIndex})
        end,
        PartitionStates0,
        PartitionStates1
    ).

-spec take_partition_states(
    TopicPartitions :: [{kafine:topic(), kafine:partition()}],
    TopicPartitionStates :: kafine_consumer:topic_partition_states()
) ->
    {
        Discard :: kafine_consumer:topic_partition_states(),
        Keep :: kafine_consumer:topic_partition_states()
    }.

take_partition_states(TopicPartitions, TopicPartitionStates) ->
    take_partition_states(TopicPartitions, #{}, TopicPartitionStates).

take_partition_states([], Take, Keep) ->
    {Take, Keep};
take_partition_states([{Topic, Partition} | TopicPartitions], Take, Keep) ->
    {Value, Keep2} = kafine_maps:take([Topic, Partition], Keep),
    Take2 = kafine_maps:put([Topic, Partition], Value, Take),
    take_partition_states(TopicPartitions, Take2, Keep2).
