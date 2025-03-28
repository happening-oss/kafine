-module(kafine_topic_options_tests).
-include_lib("eunit/include/eunit.hrl").

missing_options_test() ->
    ?assertEqual(
        #{
            initial_offset => earliest,
            offset_reset_policy => latest
        },
        kafine_topic_options:validate_options(#{})
    ),
    ok.

missing_topic_2_test() ->
    Topics = [<<"missing">>],
    ?assertEqual(
        #{<<"missing">> => #{initial_offset => earliest, offset_reset_policy => latest}},
        kafine_topic_options:validate_options(Topics, #{})
    ),
    ok.

missing_options_2_test() ->
    Topics = [<<"present">>],
    ?assertEqual(
        #{<<"present">> => #{initial_offset => earliest, offset_reset_policy => latest}},
        kafine_topic_options:validate_options(Topics, #{<<"present">> => #{}})
    ),
    ok.
