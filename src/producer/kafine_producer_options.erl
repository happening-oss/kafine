-module(kafine_producer_options).
-export([
    validate_options/1
]).

% Defaults to 5ms for kafka >= 4.0
-define(DEFAULT_LINGER_MS, 5).
% Java client default
-define(DEFAULT_MAX_BATCH_SIZE_BYTES, 16_384).
% 1Mi, Java client default
-define(DEFAULT_MAX_REQUEST_SIZE_BYTES, 1_048_576).

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
        acks => all,
        compression => none,
        linger_ms => ?DEFAULT_LINGER_MS,
        max_batch_size_bytes => ?DEFAULT_MAX_BATCH_SIZE_BYTES,
        max_request_size_bytes => ?DEFAULT_MAX_REQUEST_SIZE_BYTES,
        retry_backoff => kafine_backoff:exponential(),
        metadata => #{}
    }.

required_options() ->
    [
        acks,
        compression,
        linger_ms,
        max_batch_size_bytes,
        max_request_size_bytes,
        retry_backoff,
        metadata
    ].

validate_option(acks, Value) when Value =:= none; Value =:= leader; Value =:= all ->
    ok;
validate_option(compression, Value) when
    Value =:= none; Value =:= gzip; Value =:= snappy; Value =:= lz4; Value =:= zstd
->
    ok;
validate_option(linger_ms, Value) when is_integer(Value), Value >= 0 ->
    ok;
validate_option(max_batch_size_bytes, Value) when is_integer(Value), Value >= 0 ->
    ok;
validate_option(max_request_size_bytes, Value) when is_integer(Value), Value >= 0 ->
    % TODO: We'd actually like to enforce this being > max_batch_size_bytes
    ok;
validate_option(retry_backoff, Value) ->
    kafine_backoff:validate_options(Value);
validate_option(metadata, Value) when is_map(Value) ->
    ok;
validate_option(Key, Value) ->
    error(badarg, [Key, Value]).
