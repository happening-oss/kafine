-module(topic_consumer_callback).
-behaviour(kafine_consumer_callback).
-export([
    init/3,
    begin_record_batch/5,
    handle_record/4,
    end_record_batch/5
]).

-record(state, {
    parent :: pid()
}).

init(_Topic, _Partition, _Args = Parent) when is_pid(Parent) ->
    {ok, #state{parent = Parent}}.

begin_record_batch(
    _Topic,
    _Partition,
    _CurrentOffset,
    _Info,
    State
) ->
    {ok, State}.

handle_record(
    Topic, Partition, Message, State = #state{parent = Parent}
) ->
    Parent ! {handle_record, {Topic, Partition, Message}},
    {ok, State}.

end_record_batch(
    _Topic,
    _Partition,
    _NextOffset,
    _Info,
    State
) ->
    {ok, State}.
