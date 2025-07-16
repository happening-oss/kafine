-module(kafine_trace).
%%% Convert `gen_server` and `gen_statem` tracing into calls to `logger`.
-moduledoc false.
-export([
    debug_options/0,
    debug_options/1,
    debug_options/2
]).

-spec debug_options() -> [sys:debug_option()].
debug_options() ->
    debug_options(debug, #{}).

-spec debug_options(Metadata :: logger:metadata()) -> [sys:debug_option()].
debug_options(Metadata) ->
    debug_options(debug, Metadata).

-spec debug_options(Level :: logger:level(), Metadata :: logger:metadata()) -> [sys:debug_option()].
debug_options(Level, Metadata) ->
    maybe_debug(Level, Metadata, is_trace_enabled()).

-spec is_trace_enabled() -> true | false.
is_trace_enabled() ->
    case application:get_env(kafine, enable_trace, false) of
        true -> true;
        _ -> false
    end.

-spec maybe_debug(Level :: logger:level(), Metadata :: logger:metadata(), Enable :: true | false) ->
    [sys:debug_option()].
maybe_debug(Level, Metadata, _Enable = true) ->
    % A word of caution: You can't pass a 2-tuple (i.e. a single-item record) as the function state in 'install',
    % because OTP mistakes {Func, {record, Field}} for {FuncId, {Func, FuncState}}, which is how it internally stores
    % {FuncId, Func, FuncState}; see https://github.com/erlang/otp/blob/OTP-27.3.2/lib/stdlib/src/sys.erl#L1067
    %
    % To fix this, we're careful to use the 3-tuple form.

    {FuncId, Func, FuncState} = kafine_trace_logger:init(Level, Metadata),
    [{install, {FuncId, Func, FuncState}}];
maybe_debug(_Level, _Metadata, _Enable = false) ->
    % If 'enable_trace' is false, use an empty list. gen_server and gen_statem will spot this and optimise for it.
    [].
