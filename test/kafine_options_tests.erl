-module(kafine_options_tests).
-include_lib("eunit/include/eunit.hrl").

default_options_test() ->
    ?assertEqual(
        #{name => <<"Bob">>, age => 34, iq => 100},
        kafine_options:validate_options(
            #{age => 34, iq => 100},
            #{name => <<"Bob">>, age => 32},
            [],
            false
        )
    ).

missing_required_option_test() ->
    % One of the required options is in the defaults; the other isn't.
    ?assertError(
        {missing_options, [age]},
        kafine_options:validate_options(
            #{iq => 100},
            #{name => <<"Bob">>},
            [name, age],
            false
        )
    ).

unexpected_option_test() ->
    ?assertError(
        {unexpected_options, [locale]},
        kafine_options:validate_options(
            #{name => <<"Bob">>, age => 32, locale => <<"uk">>},
            #{},
            [name, age],
            true
        )
    ).

non_strict_extra_option_test() ->
    ?assertEqual(
        #{default => added, extra => allowed},
        kafine_options:validate_options(#{extra => allowed}, #{default => added}, [], false)
    ).

validate_fun_test() ->
    ?assertError(
        badarg,
        kafine_options:validate_options(
            #{name => <<"Eddy">>, age => 12},
            #{},
            [name, age],
            true,
            fun
                (age, Age) when Age < 18 -> error(badarg);
                (_, _) -> ok
            end
        )
    ).
