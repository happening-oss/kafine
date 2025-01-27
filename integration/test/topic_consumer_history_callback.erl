-module(topic_consumer_history_callback).
-behaviour(kafine_consumer_callback).
-export([
    init/3,
    begin_record_batch/5,
    handle_record/4,
    end_record_batch/5,
    handle_parity/4
]).

-record(state, {
    history :: pid()
}).

init(_T, _P, History) when is_pid(History) ->
    State = #state{history = History},
    {ok, State}.

begin_record_batch(
    _Topic,
    _Partition,
    _CurrentOffset,
    _Info,
    State
) ->
    {ok, State}.

handle_record(T, P, M, State = #state{history = History}) ->
    topic_consumer_history:append(History, {{T, P}, {record, M}}),
    {ok, State}.

end_record_batch(
    _Topic,
    _Partition,
    _NextOffset,
    _Info,
    State
) ->
    {ok, State}.

handle_parity(T, P, O, State = #state{history = History}) ->
    topic_consumer_history:append(History, {{T, P}, {parity, O}}),
    {ok, State}.
