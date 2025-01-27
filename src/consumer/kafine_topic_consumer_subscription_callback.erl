-module(kafine_topic_consumer_subscription_callback).
-behaviour(kafine_subscription_callback).

-export([
    init/1,
    subscribe_partitions/3,
    unsubscribe_partitions/1
]).

-record(state, {
    consumer :: kafine_consumer:ref(),
    topic_options :: kafine:topic_options()
}).

init([Consumer, TopicOptions]) ->
    {ok, #state{consumer = Consumer, topic_options = TopicOptions}}.

subscribe_partitions(
    _Coordinator,
    AssignedPartitions,
    State = #state{consumer = Consumer, topic_options = TopicOptions}
) ->
    kafine_consumer:subscribe(Consumer, make_subscription(AssignedPartitions, TopicOptions)),
    {ok, State}.

unsubscribe_partitions(State = #state{consumer = Consumer}) ->
    kafine_consumer:unsubscribe_all(Consumer),
    {ok, State}.

make_subscription(AssignedPartitions, AllTopicOptions) ->
    Fun = fun(TopicName, Partitions) ->
        InitialOffset = -1,
        MakeOffset = fun(Partition, OffsetAcc) ->
            OffsetAcc#{ Partition => InitialOffset }
        end,
        Offsets = lists:foldl(MakeOffset, #{}, Partitions),
        TopicOptions = maps:get(TopicName, AllTopicOptions, #{}),
        {TopicOptions, Offsets}
    end,
    Sub = maps:map(Fun, AssignedPartitions),
    Sub.
