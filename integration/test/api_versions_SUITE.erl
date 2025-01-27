-module(api_versions_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

all() ->
    [
        api_versions
    ].

suite() ->
    [
        {require, bootstrap_server}
    ].

-define(CLIENT_ID, atom_to_binary(?MODULE)).

parse_broker(Broker) when is_list(Broker) ->
    [Host, Port] = string:split(Broker, ":"),
    #{host => list_to_binary(Host), port => list_to_integer(Port)}.

api_versions(_Config) ->
    BootstrapServer = ct:get_config(bootstrap_server),
    Bootstrap = parse_broker(BootstrapServer),
    {ok, C} = kafine_connection:start_link(Bootstrap, #{client_id => ?CLIENT_ID}),

    ?assertMatch(
        {ok, #{error_code := ?NONE, api_keys := [_ | _]}},
        kafine_connection:call(
            C,
            fun api_versions_request:encode_api_versions_request_2/1,
            #{},
            fun api_versions_response:decode_api_versions_response_2/1
        )
    ),
    ok.
