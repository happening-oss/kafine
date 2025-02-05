-module(kafine_consumer_callback_logger).
-behaviour(kafine_consumer_callback).
-export([
    init/3,
    begin_record_batch/5,
    handle_record/4,
    end_record_batch/5
]).

-include_lib("kernel/include/logger.hrl").

-record(state, {
    formatter,
    stage = first_fetch,
    current_offset = -1
}).

% `kafine_consumer_callback_logger` allows you to pass a formatting function. So, for example, if your messages are
% binary-term-encoded, you might use this:

% ```erlang
% Callback =
%     {kafine_consumer_callback_logger, #{
%         formatter =>
%             fun(K, V, H) ->
%                 io_lib:format("~s = ~p (~p)", [K, binary_to_term(V), H])
%             end
%     }}.
% ```

init(_T, _P, #{formatter := Formatter}) when is_function(Formatter, 3) ->
    State = #state{formatter = Formatter},
    {ok, State};
init(_T, _P, #{formatter := _}) ->
    error(badarg);
init(_T, _P, _Args) ->
    State = #state{formatter = fun default_formatter/3},
    {ok, State}.

begin_record_batch(_Topic, _Partition, CurrentOffset, _Info, State) ->
    {ok, State#state{current_offset = CurrentOffset}}.

handle_record(
    Topic,
    Partition,
    _Message = #{
        offset := Offset,
        timestamp := Timestamp,
        key := Key,
        value := Value,
        headers := Headers
    },
    State = #state{formatter = Formatter}
) ->
    ?LOG_INFO("~s:~B#~B ~p :: ~s", [
        Topic, Partition, Offset, Timestamp, Formatter(Key, Value, Headers)
    ]),
    {ok, State}.

end_record_batch(
    Topic,
    Partition,
    NextOffset,
    _Info = #{high_watermark := HighWatermark},
    State = #state{current_offset = CurrentOffset}
) when NextOffset == HighWatermark, CurrentOffset /= NextOffset ->
    report_parity(Topic, Partition, NextOffset),
    {ok, State#state{stage = subsequent_fetches}};
end_record_batch(
    Topic,
    Partition,
    NextOffset,
    _Info = #{high_watermark := HighWatermark},
    State = #state{stage = Stage}
) when NextOffset == HighWatermark, Stage == first_fetch ->
    report_parity(Topic, Partition, NextOffset),
    {ok, State#state{stage = subsequent_fetches}};
end_record_batch(
    _Topic,
    _Partition,
    _NextOffset,
    _Info,
    State
) ->
    {ok, State}.

report_parity(Topic, Partition, NextOffset) ->
    ?LOG_INFO("Reached end of topic ~s [~B] at offset ~B", [Topic, Partition, NextOffset]),
    ok.

default_formatter(Key, Value, Headers) ->
    io_lib:format("~s = ~p (~p)", [Key, Value, Headers]).
