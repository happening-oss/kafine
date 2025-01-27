-module(kafine_consumer_options).
-export([
    validate_options/1
]).
% kcat defaults
-define(MAX_WAIT_TIME_MS, 500).
-define(MIN_BYTES, 1).
% kcat default: 50MiB.
-define(MAX_BYTES, 52_428_800).
% kcat default: 1MiB
-define(PARTITION_MAX_BYTES, 1_048_576).
-define(DEFAULT_ISOLATION_LEVEL, read_uncommitted).

validate_options(Options) ->
    kafine_options:validate_options(
        Options,
        #{
            max_wait_ms => ?MAX_WAIT_TIME_MS,
            min_bytes => ?MIN_BYTES,
            max_bytes => ?MAX_BYTES,
            partition_max_bytes => ?PARTITION_MAX_BYTES,
            isolation_level => ?DEFAULT_ISOLATION_LEVEL
        },
        [max_wait_ms, min_bytes, max_bytes, partition_max_bytes, isolation_level],
        true,
        fun validate_option/2
    ).

validate_option(max_wait_ms, Value) when is_integer(Value), Value >= 0 ->
    ok;
validate_option(min_bytes, Value) when is_integer(Value), Value >= 0 ->
    ok;
validate_option(max_bytes, Value) when is_integer(Value), Value >= 0 ->
    ok;
validate_option(partition_max_bytes, Value) when is_integer(Value), Value >= 0 ->
    ok;
validate_option(isolation_level, Value) when Value == read_uncommitted; Value == read_committed ->
    ok;
validate_option(Key, Value) ->
    error(badarg, [Key, Value]).
