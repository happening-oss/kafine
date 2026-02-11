-module(kafine_backoff).

-export([
    fixed/0,
    fixed/1
]).
-export([
    init/1,
    backoff/1,
    reset/1
]).
-export([validate_options/1]).

-export_type([
    fixed_config/0,
    config/0,
    state/0
]).

-type fixed_config() :: #{type := fixed, interval_ms := non_neg_integer()}.

-type fixed_state() :: #{config := fixed_config()}.

-type config() :: fixed_config().

-opaque state() :: fixed_state().

-define(DEFAULT_MAX_RECONNECT_INTERVAL_MS, 1_000).

-spec fixed() -> fixed_config().
fixed() ->
    fixed(?DEFAULT_MAX_RECONNECT_INTERVAL_MS).

-spec fixed(IntervalMs :: non_neg_integer()) -> fixed_config().
fixed(IntervalMs) when is_integer(IntervalMs), IntervalMs >= 0 ->
    #{type => fixed, interval_ms => IntervalMs}.

-spec init(Config :: config()) -> state().
init(Config = #{type := fixed}) ->
    #{config => Config}.

-spec backoff(State :: StateT) -> {BackoffMs :: non_neg_integer(), State :: StateT} when
    StateT :: state().
backoff(State = #{config := #{type := fixed, interval_ms := IntervalMs}}) ->
    {IntervalMs, State}.

-spec reset(State :: StateT) -> StateT when StateT :: state().
reset(State = #{config := #{type := fixed}}) ->
    State.

validate_options(Config = #{type := Type}) ->
    kafine_options:validate_options(
        Config,
        default_options(Type),
        required_keys(Type),
        true,
        fun(Key, Value) -> validate_option(Type, Key, Value) end
    ).

default_options(fixed) ->
    #{interval_ms => ?DEFAULT_MAX_RECONNECT_INTERVAL_MS};
default_options(Type) ->
    error(badarg, [type, Type]).

required_keys(fixed) ->
    [type, interval_ms].

validate_option(_, interval_ms, IntervalMs) when is_integer(IntervalMs), IntervalMs >= 0 ->
    ok;
validate_option(Type, type, Type) ->
    ok;
validate_option(Type, Key, Value) ->
    error(badarg, [Type, Key, Value]).
