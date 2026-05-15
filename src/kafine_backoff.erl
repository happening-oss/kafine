-module(kafine_backoff).

-export([
    fixed/0,
    fixed/1,
    exponential/0,
    exponential/1,
    exponential/2
]).
-export([
    init/1,
    backoff/1
]).
-export([validate_options/1]).

-export_type([
    fixed_config/0,
    exponential_config/0,
    config/0,
    state/0
]).

-type fixed_config() :: #{type := fixed, interval_ms := non_neg_integer()}.
-type exponential_config() :: #{
    type := exponential,
    initial_ms := non_neg_integer(),
    max_ms := non_neg_integer() | infinity,
    max_count := non_neg_integer() | infinity
}.

-type fixed_state() :: #{config := fixed_config()}.
-type exponential_state() :: #{config := exponential_config(), count := non_neg_integer()}.

-type config() :: fixed_config() | exponential_config().

-opaque state() :: fixed_state() | exponential_state().

-define(DEFAULT_INITIAL_RETRY_INTERVAL_MS, 100).
-define(DEFAULT_MAX_RETRY_INTERVAL_MS, 1_000).

-spec fixed() -> fixed_config().
fixed() ->
    fixed(?DEFAULT_MAX_RETRY_INTERVAL_MS).

-spec fixed(IntervalMs :: non_neg_integer()) -> fixed_config().
fixed(IntervalMs) when is_integer(IntervalMs), IntervalMs >= 0 ->
    #{type => fixed, interval_ms => IntervalMs}.

-spec exponential() -> exponential_config().
exponential() ->
    exponential(#{}).

-spec exponential(Options :: map()) -> exponential_config().
exponential(Options) when is_map(Options) ->
    validate_options(maps:put(type, exponential, Options)).

-spec exponential(InitialMs :: non_neg_integer(), MaxMs :: non_neg_integer() | infinity) ->
    exponential_config().
exponential(InitialMs, MaxMs) ->
    exponential(#{initial_ms => InitialMs, max_ms => MaxMs}).

-spec init(Config :: config()) -> state().
init(Config = #{type := fixed}) ->
    #{config => Config};
init(Config = #{type := exponential}) ->
    #{config => Config, count => 0}.

-spec backoff(State :: StateT) ->
    {BackoffMs :: non_neg_integer(), State :: StateT} | limit_exceeded
when
    StateT :: state().
backoff(State = #{config := #{type := fixed, interval_ms := IntervalMs}}) ->
    {IntervalMs, State};
backoff(
    #{
        config := #{type := exponential, max_count := MaxCount}, count := Count
    }
) when Count >= MaxCount ->
    limit_exceeded;
backoff(
    State = #{
        config := #{type := exponential, initial_ms := InitialMs, max_ms := MaxMs}, count := Count
    }
) ->
    % (2 bsl (Count - 1)) is equivalent to math:pow(2, Count)
    IntervalMs0 = InitialMs * (2 bsl (Count - 1)),
    IntervalMs =
        case MaxMs of
            infinity -> IntervalMs0;
            _ -> min(IntervalMs0, MaxMs)
        end,
    NewState = State#{count => Count + 1},
    {IntervalMs, NewState}.

validate_options(Config = #{type := Type}) ->
    kafine_options:validate_options(
        Config,
        default_options(Type),
        required_keys(Type),
        true,
        fun(Key, Value) -> validate_option(Type, Key, Value) end
    ).

default_options(fixed) ->
    #{interval_ms => ?DEFAULT_MAX_RETRY_INTERVAL_MS};
default_options(exponential) ->
    #{
        initial_ms => ?DEFAULT_INITIAL_RETRY_INTERVAL_MS,
        max_ms => ?DEFAULT_MAX_RETRY_INTERVAL_MS,
        max_count => infinity
    };
default_options(Type) ->
    error(badarg, [type, Type]).

required_keys(fixed) ->
    [type, interval_ms];
required_keys(exponential) ->
    [type, initial_ms, max_ms, max_count].

validate_option(Type, type, Type) when Type =:= fixed; Type =:= exponential ->
    ok;
validate_option(fixed, interval_ms, IntervalMs) when is_integer(IntervalMs), IntervalMs >= 0 ->
    ok;
validate_option(exponential, initial_ms, IntervalMs) when is_integer(IntervalMs), IntervalMs >= 0 ->
    ok;
validate_option(exponential, max_ms, IntervalMs) when is_integer(IntervalMs), IntervalMs >= 0 ->
    ok;
validate_option(exponential, max_ms, infinity) ->
    ok;
validate_option(exponential, max_count, Count) when is_integer(Count), Count >= 0 ->
    ok;
validate_option(exponential, max_count, infinity) ->
    ok;
validate_option(Type, Key, Value) ->
    error(badarg, [Type, Key, Value]).
