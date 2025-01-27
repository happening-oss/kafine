-module(kafine_topic_options_tests).
-include_lib("eunit/include/eunit.hrl").

missing_options_test() ->
    ?assertEqual(
        #{offset_reset_policy => earliest}, kafine_topic_options:validate_options(#{})
    ),
    ok.

missing_topic_2_test() ->
    % TODO: These tests only make sense for separate Topic, TopicOptions; we're gonna merge them.
    Topics = [<<"missing">>],
    ?assertEqual(
        #{<<"missing">> => #{offset_reset_policy => earliest}},
        kafine_topic_options:validate_options(Topics, #{})
    ),
    ok.

missing_options_2_test() ->
    Topics = [<<"present">>],
    ?assertEqual(
        #{<<"present">> => #{offset_reset_policy => earliest}},
        kafine_topic_options:validate_options(Topics, #{<<"present">> => #{}})
    ),
    ok.
