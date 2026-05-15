-module(topic_consumer_callback).
-behaviour(kafine_consumer_callback).
-export([
    init/3,
    handle_partition_data/4
]).

-record(state, {
    parent :: pid()
}).

init(_Topic, _Partition, _Args = Parent) when is_pid(Parent) ->
    {ok, #state{parent = Parent}}.

handle_partition_data(
    Topic, Partition, PartitionData, State = #state{parent = Parent}
) ->
    Parent ! {handle_partition_data, {Topic, Partition, PartitionData}},
    {ok, State}.
