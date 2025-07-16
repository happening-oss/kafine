-module(kafine_connection_options_tests).
-include_lib("eunit/include/eunit.hrl").

no_options_test() ->
    Validated = kafine_connection_options:validate_options(#{}),
    ?assertMatch(#{client_id := <<"kafine">>, connect_timeout := infinity, metadata := #{}}, Validated),
    ok.

client_id_test() ->
    Validated = kafine_connection_options:validate_options(#{
        client_id => <<"test">>
    }),
    ?assertMatch(#{client_id := <<"test">>, metadata := #{}}, Validated),
    ok.

bad_connect_timeout_test() ->
    ?assertError(
        badarg,
        kafine_connection_options:validate_options(#{
            connect_timeout => bad
        })
    ),
    ok.

bad_client_id_test() ->
    ?assertError(
        badarg,
        kafine_connection_options:validate_options(#{
            client_id => bad
        })
    ),
    ok.

metadata_with_valid_keys_test() ->
    Validated = kafine_connection_options:validate_options(#{metadata => #{a => a, b => b}}),
    ?assertMatch(#{client_id := <<"kafine">>, metadata := #{a := a, b := b}}, Validated),
    ok.

metadata_with_no_keys_test() ->
    Validated = kafine_connection_options:validate_options(#{metadata => #{}}),
    ?assertMatch(#{client_id := <<"kafine">>, metadata := #{}}, Validated),
    ok.

no_metadata_test() ->
    Validated = kafine_connection_options:validate_options(#{}),
    ?assertMatch(#{client_id := <<"kafine">>, metadata := #{}}, Validated),
    ok.
