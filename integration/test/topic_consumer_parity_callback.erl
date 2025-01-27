-module(topic_consumer_parity_callback).
-behaviour(kafine_consumer_callback).
-export([
    init/3,
    begin_record_batch/5,
    handle_record/4,
    end_record_batch/5
]).

% TODO: This callback belongs inside kafine; otherwise we're just testing the tests. For now, however, it can stay here.
-record(state, {
    callback,
    callback_state,
    stage = first_fetch,
    current_offset = -1
}).

init(T, P, {Callback, Args}) ->
    {ok, CallbackState} = Callback:init(T, P, Args),
    State = #state{callback = Callback, callback_state = CallbackState},
    {ok, State}.

begin_record_batch(
    Topic,
    Partition,
    CurrentOffset,
    Info,
    State = #state{callback = Callback, callback_state = CallbackState}
) ->
    {ok, CallbackState2} = Callback:begin_record_batch(
        Topic, Partition, CurrentOffset, Info, CallbackState
    ),
    {ok, State#state{current_offset = CurrentOffset, callback_state = CallbackState2}}.

handle_record(
    Topic,
    Partition,
    Message,
    State = #state{callback = Callback, callback_state = CallbackState}
) ->
    {ok, CallbackState2} = Callback:handle_record(Topic, Partition, Message, CallbackState),
    {ok, State#state{callback_state = CallbackState2}}.

end_record_batch(
    Topic,
    Partition,
    NextOffset,
    Info,
    State = #state{callback = Callback, callback_state = CallbackState}
) ->
    % TODO: Parity caused by the partition being empty?
    {ok, CallbackState2} = Callback:end_record_batch(
        Topic, Partition, NextOffset, Info, CallbackState
    ),
    State2 = State#state{callback_state = CallbackState2},
    check_parity(Topic, Partition, NextOffset, Info, State2).

check_parity(
    Topic,
    Partition,
    NextOffset,
    _Info = #{high_watermark := HighWatermark},
    State = #state{
        current_offset = CurrentOffset, callback = Callback, callback_state = CallbackState
    }
) when NextOffset == HighWatermark, CurrentOffset /= NextOffset ->
    {ok, CallbackState2} = Callback:handle_parity(Topic, Partition, NextOffset, CallbackState),
    {ok, State#state{callback_state = CallbackState2}};
check_parity(
    Topic,
    Partition,
    NextOffset,
    _Info = #{high_watermark := HighWatermark},
    State = #state{stage = Stage, callback = Callback, callback_state = CallbackState}
) when NextOffset == HighWatermark, Stage == first_fetch ->
    {ok, CallbackState2} = Callback:handle_parity(Topic, Partition, NextOffset, CallbackState),
    {ok, State#state{callback_state = CallbackState2, stage = subsequent_fetches}};
check_parity(_Topic, _Partition, _NextOffset, _Info, State) ->
    {ok, State}.
