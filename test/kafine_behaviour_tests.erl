-module(kafine_behaviour_tests).
-include_lib("eunit/include/eunit.hrl").

%% We want to make it easy to diagnose problems with the user-provided callback module.

not_an_atom_test() ->
    % You've passed something that's not an atom.
    ?assertError(
        badarg,
        kafine_behaviour:verify_callbacks_exported(kafine_consumer_callback, #{})
    ).

not_a_module_test() ->
    % You've passed a valid atom, but we can't find a module with that name.
    ?assertError(
        {not_loaded, _},
        kafine_behaviour:verify_callbacks_exported(
            gen_server, 'atom unlikely to be a module'
        )
    ).

valid_callback_test() ->
    % The module correctly exports the callbacks specified in the behaviour.
    ?assertEqual(
        ok,
        kafine_behaviour:verify_callbacks_exported(
            gen_server, kafine_behaviour_test_ok
        )
    ).

function_not_exported_test() ->
    % The module doesn't export the callbacks specified in the behaviour.
    Module = kafine_behaviour_test_fail,
    % kafine_behaviour_test_fail:init/1 exists, so we should fail for handle_call/3 and handle_cast/2.
    ?assertError(
        {not_exported, #{
            module := Module,
            behaviour := gen_server,
            missing_callbacks := [{handle_call, 3}, {handle_cast, 2}]
        }},
        kafine_behaviour:verify_callbacks_exported(
            gen_server, Module
        )
    ).

not_a_behaviour_test() ->
    % Did you pass a behaviour/callback module that doesn't define any callbacks? That's more of an internal problem
    % than a usage problem.
    ?assertError(
        {not_a_behaviour, ?MODULE},
        kafine_behaviour:verify_callbacks_exported(?MODULE, ?MODULE)
    ).
