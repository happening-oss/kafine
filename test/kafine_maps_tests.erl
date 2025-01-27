-module(kafine_maps_tests).
-include_lib("eunit/include/eunit.hrl").

-define(UNEXPECTED, fun(_) -> error(unexpected) end).

update_with1_test_() ->
    % With a single key, this should be identical to maps:update_with/4.
    [
        {"initially empty map; initial item is added",
            ?_assertEqual(#{k1 => init}, kafine_maps:update_with([k1], ?UNEXPECTED, init, #{}))},
        {"k1 exists; item is updated",
            ?_assertEqual(
                #{k1 => second},
                kafine_maps:update_with([k1], fun(first) -> second end, ignored, #{k1 => first})
            )}
    ].

update_with2_test_() ->
    [
        {"initially empty map",
            ?_assertEqual(
                #{k1 => #{k2 => init}},
                kafine_maps:update_with([k1, k2], ?UNEXPECTED, init, #{})
            )},
        {"k1 exists, points to empty map",
            ?_assertEqual(
                #{k1 => #{k2 => init}},
                kafine_maps:update_with([k1, k2], ?UNEXPECTED, init, #{k1 => #{}})
            )},
        {"k1 exists, points to map without k2 (k2a untouched)",
            ?_assertEqual(
                #{k1 => #{k2 => init, k2a => value}},
                kafine_maps:update_with([k1, k2], ?UNEXPECTED, init, #{
                    k1 => #{k2a => value}
                })
            )},
        {"k1, k2 both exist (k1a, k2a untouched)",
            ?_assertEqual(
                #{k1 => #{k2 => [v2y, v2x], k2a => v2a}, k1a => v1a},
                kafine_maps:update_with([k1, k2], fun(Values) -> [v2y | Values] end, init, #{
                    k1 => #{k2 => [v2x], k2a => v2a}, k1a => v1a
                })
            )}
    ].

update_with2_no_init_test_() ->
    [
        {"initially empty map, should throw",
            ?_assertError(
                {badkey, k1},
                kafine_maps:update_with([k1, k2], ?UNEXPECTED, #{})
            )},
        {"k1 exists, points to empty map, should throw",
            ?_assertError(
                {badkey, k2},
                kafine_maps:update_with([k1, k2], ?UNEXPECTED, #{k1 => #{}})
            )},
        {"k1 exists, points to map without k2, should throw",
            ?_assertError(
                {badkey, k2},
                kafine_maps:update_with([k1, k2], ?UNEXPECTED, #{
                    k1 => #{k2a => value}
                })
            )},

        {"k1, k2 both exist (k1a, k2a untouched)",
            ?_assertEqual(
                #{k1 => #{k2 => [v2y, v2x], k2a => v2a}, k1a => v1a},
                kafine_maps:update_with([k1, k2], fun(Values) -> [v2y | Values] end, #{
                    k1 => #{k2 => [v2x], k2a => v2a}, k1a => v1a
                })
            )}
    ].

put2_test_() ->
    [
        {"neither key", ?_assertEqual(#{k1 => #{k2 => v}}, kafine_maps:put([k1, k2], v, #{}))},
        {"k1 present",
            ?_assertEqual(#{k1 => #{k2 => v}}, kafine_maps:put([k1, k2], v, #{k1 => #{}}))},
        {"k1, k2 present",
            ?_assertEqual(#{k1 => #{k2 => v}}, kafine_maps:put([k1, k2], v, #{k1 => #{k2 => old}}))},
        {"neither key, but other keys",
            ?_assertEqual(
                #{k1 => #{k2 => v}, q1 => #{q2 => r}},
                kafine_maps:put([k1, k2], v, #{q1 => #{q2 => r}})
            )},
        {"k1 present, but other keys",
            ?_assertEqual(
                #{k1 => #{k2 => v}, q1 => #{q2 => r}},
                kafine_maps:put([k1, k2], v, #{k1 => #{}, q1 => #{q2 => r}})
            )},
        {"k1, k2 present, but other keys",
            ?_assertEqual(
                #{k1 => #{k2 => v}, q1 => #{q2 => r}},
                kafine_maps:put([k1, k2], v, #{k1 => #{k2 => old}, q1 => #{q2 => r}})
            )}
    ].

% This one's an example of expected usage: group Kafka partitions and topics by leader.
group_partitions_by_leader_test() ->
    Topics = [
        #{
            name => <<"cats">>,
            partitions => [
                #{partition_index => 0, leader_id => 101},
                #{partition_index => 1, leader_id => 103},
                #{partition_index => 2, leader_id => 103},
                #{partition_index => 3, leader_id => 102}
            ]
        },
        #{
            name => <<"dogs">>,
            partitions => [
                #{partition_index => 0, leader_id => 102},
                #{partition_index => 1, leader_id => 102},
                #{partition_index => 2, leader_id => 101},
                #{partition_index => 3, leader_id => 101},
                #{partition_index => 4, leader_id => 102}
            ]
        }
    ],

    % Note: I considered implementing 'nested_group_by', allowing arbitrarily-nested lists-of-maps-containing-lists, and
    % generating arbitrarily-nested maps-of-maps. It would probably have been easy for the user to understand, but would
    % have been kinda complicated to write, and essentially impossible to understand once written. So I didn't.
    ByLeader = lists:foldl(
        fun(#{name := Topic, partitions := Partitions}, Acc1) ->
            lists:foldl(
                fun(#{partition_index := PartitionIndex, leader_id := LeaderId}, Acc2) ->
                    kafine_maps:update_with(
                        [LeaderId, Topic],
                        fun(PartitionIndices) -> [PartitionIndex | PartitionIndices] end,
                        [PartitionIndex],
                        Acc2
                    )
                end,
                Acc1,
                Partitions
            )
        end,
        #{},
        Topics
    ),
    ?assertEqual(
        #{
            101 => #{<<"cats">> => [0], <<"dogs">> => [3, 2]},
            102 => #{<<"cats">> => [3], <<"dogs">> => [4, 1, 0]},
            103 => #{<<"cats">> => [2, 1]}
        },
        ByLeader
    ).

take1_test_() ->
    [
        {"key exists",
            ?_assertEqual({v1, #{k2 => v2}}, kafine_maps:take([k1], #{k1 => v1, k2 => v2}))}
    ].

take2_test_() ->
    [
        {"keys exist",
            ?_assertEqual(
                {v, #{}},
                kafine_maps:take([a, b], #{a => #{b => v}})
            )},

        {"other keys exist, are preserved",
            ?_assertEqual(
                {v11, #{
                    k1 => #{k12 => v12},
                    k2 => #{k21 => v21}
                }},
                kafine_maps:take([k1, k11], #{
                    k1 => #{k11 => v11, k12 => v12},
                    k2 => #{k21 => v21}
                })
            )},

        {"key doesn't exist", ?_assertEqual(error, kafine_maps:take([x], #{a => #{b => v}}))},
        {"key doesn't exist", ?_assertEqual(error, kafine_maps:take([a, x], #{a => #{b => v}}))}
    ].

get1_test_() ->
    [
        {"key exists", ?_assertEqual(v1, kafine_maps:get([k1], #{k1 => v1, k2 => v2}))},
        {"key doesn't exist",
            ?_assertError({badkey, nope}, kafine_maps:get([nope], #{k1 => v1, k2 => v2}))}
    ].

get2_test_() ->
    [
        {"keys exist",
            ?_assertEqual(v11, kafine_maps:get([k1, k11], #{k1 => #{k11 => v11}, k2 => v2}))},
        {"top key doesn't exist",
            ?_assertError(
                {badkey, nope}, kafine_maps:get([nope], #{k1 => #{k11 => v11}, k2 => v2})
            )},

        % Note: Throwing {badkey, nope} doesn't tell you which level the key was missing at. We should include the full
        % path.
        %
        % But: if we throw {badkey, [k1, nope]}, it's not obvious whether we're telling you which key didn't exist, or
        % just including the entire list (if it's the last key that didn't exist; it's more obvious otherwise).
        %
        % So, maybe, we should throw {badkey, [k1], nope, []}, which would tell you which keys got found, which one
        % didn't, and the ones we didn't use.
        %
        % But: lots of housekeeping for the (hopefully) rare case.
        {"second key doesn't exist",
            ?_assertError(
                {badkey, nope}, kafine_maps:get([k1, nope], #{k1 => #{k11 => v11}, k2 => v2})
            )}
    ].

get1_default_test_() ->
    [
        {"key exists",
            ?_assertEqual(v1, kafine_maps:get([k1], #{k1 => v1, k2 => v2}, default_value))},
        {"key doesn't exist",
            ?_assertEqual(
                default_value, kafine_maps:get([nope], #{k1 => v1, k2 => v2}, default_value)
            )}
    ].

get2_default_test_() ->
    [
        {"keys exist",
            ?_assertEqual(
                v11, kafine_maps:get([k1, k11], #{k1 => #{k11 => v11}, k2 => v2}, default_value)
            )},
        {"single key doesn't exist",
            ?_assertEqual(
                default_value,
                kafine_maps:get([nope], #{k1 => #{k11 => v11}, k2 => v2}, default_value)
            )},
        {"top key doesn't exist",
            ?_assertEqual(
                default_value,
                kafine_maps:get([nope, whatever], #{k1 => #{k11 => v11}, k2 => v2}, default_value)
            )},
        {"second key doesn't exist",
            ?_assertEqual(
                default_value,
                kafine_maps:get([k1, nope], #{k1 => #{k11 => v11}, k2 => v2}, default_value)
            )}
    ].

remove1_test_() ->
    [
        {"key exists",
            ?_assertEqual(
                #{k2 => v2},
                kafine_maps:remove([k1], #{k1 => v1, k2 => v2})
            )},
        {"key doesn't exist",
            ?_assertEqual(
                #{k1 => v1, k2 => v2},
                kafine_maps:remove([k3], #{k1 => v1, k2 => v2})
            )}
    ].

remove2_test_() ->
    [
        {"keys exist",
            ?_assertEqual(
                #{k1 => #{k12 => v12}, k2 => v2},
                kafine_maps:remove([k1, k11], #{k1 => #{k11 => v11, k12 => v12}, k2 => v2})
            )},
        {"keys exist; empties map",
            ?_assertEqual(
                #{},
                kafine_maps:remove([k1], #{k1 => #{k11 => v11}})
            )},
        {"top key exists; remove everything below it",
            ?_assertEqual(
                #{k2 => v2},
                kafine_maps:remove([k1], #{k1 => #{k11 => v11}, k2 => v2})
            )},
        {"single key doesn't exist",
            ?_assertEqual(
                #{k1 => #{k11 => v11}, k2 => v2},
                kafine_maps:remove([k3], #{k1 => #{k11 => v11}, k2 => v2})
            )},
        {"top key doesn't exist",
            ?_assertEqual(
                #{k1 => #{k11 => v11}, k2 => v2},
                kafine_maps:remove([k3, k4], #{k1 => #{k11 => v11}, k2 => v2})
            )},
        {"second key doesn't exist",
            ?_assertEqual(
                #{k1 => #{k11 => v11}, k2 => v2},
                kafine_maps:remove([k1, k12], #{k1 => #{k11 => v11}, k2 => v2})
            )}
    ].
