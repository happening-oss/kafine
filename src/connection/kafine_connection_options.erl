-module(kafine_connection_options).
-export([
    validate_options/1
]).

-define(DEFAULT_CLIENT_ID, <<"kafine">>).
-define(DEFAULT_METADATA, #{}).

validate_options(Options) ->
    kafine_options:validate_options(
        Options,
        default_options(),
        [],
        false,
        fun validate_option/2
    ).

default_options() ->
    #{
        transport => gen_tcp,
        transport_options => [],
        client_id => ?DEFAULT_CLIENT_ID,
        connect_timeout => infinity,
        metadata => ?DEFAULT_METADATA
    }.

validate_option(transport, Transport) when Transport =:= gen_tcp; Transport =:= ssl ->
    ok;
validate_option(transport_options, _) ->
    ok;
validate_option(client_id, Value) when is_binary(Value) ->
    ok;
validate_option(connect_timeout, infinity) ->
    ok;
validate_option(connect_timeout, Timeout) when is_integer(Timeout) ->
    ok;
validate_option(metadata, Value) when is_map(Value) ->
    ok;
validate_option(Key, Value) ->
    error(badarg, [Key, Value]).
