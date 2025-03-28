-module(kafine_membership_options).
-export([
    validate_options/1
]).

-define(DEFAULT_ASSIGNOR, kafine_range_assignor).

% Arbitrary number; same as kcat default.
-define(DEFAULT_HEARTBEAT_INTERVAL_MS, 3_000).

-define(DEFAULT_SESSION_TIMEOUT_MS, 45_000).
-define(DEFAULT_REBALANCE_TIMEOUT_MS, 90_000).

validate_options(Options) ->
    kafine_options:validate_options(
        Options,
        default_options(),
        required_options(),
        true,
        fun validate_option/2
    ).

default_options() ->
    #{
        assignor => ?DEFAULT_ASSIGNOR,
        heartbeat_interval_ms => ?DEFAULT_HEARTBEAT_INTERVAL_MS,
        session_timeout_ms => ?DEFAULT_SESSION_TIMEOUT_MS,
        rebalance_timeout_ms => ?DEFAULT_REBALANCE_TIMEOUT_MS
    }.

required_options() ->
    [
        assignor,
        subscription_callback,
        assignment_callback,
        heartbeat_interval_ms,
        session_timeout_ms,
        rebalance_timeout_ms
    ].

validate_option(assignor, Module) ->
    ok = kafine_behaviour:verify_callbacks_exported(kafine_assignor, Module);
validate_option(subscription_callback, {Module, _Args}) ->
    ok = kafine_behaviour:verify_callbacks_exported(kafine_subscription_callback, Module);
validate_option(assignment_callback, {Module, _Args}) ->
    ok = kafine_behaviour:verify_callbacks_exported(kafine_assignment_callback, Module);
validate_option(heartbeat_interval_ms, Value) when is_integer(Value) ->
    ok;
validate_option(session_timeout_ms, Value) when is_integer(Value) ->
    ok;
validate_option(rebalance_timeout_ms, Value) when is_integer(Value) ->
    ok.
