-module(kafine_round_robin_tests).
-include_lib("eunit/include/eunit.hrl").

default_fold_fun(Target, Item, Acc) -> {take, [{Target, Item} | Acc]}.

no_items_test() ->
    ?assertEqual([], kafine_round_robin:fold(fun default_fold_fun/3, [], [a, b], [])).

no_targets_test() ->
    ?assertEqual([], kafine_round_robin:fold(fun default_fold_fun/3, [], [], [1, 2, 3])).

equal_items_and_targets_test() ->
    ?assertEqual(
        [{a, 1}, {b, 2}, {c, 3}],
        % Because default_fold_fun/3 uses [X | Acc], the result is reversed.
        lists:reverse(
            kafine_round_robin:fold(fun default_fold_fun/3, [], [a, b, c], [1, 2, 3])
        )
    ).

more_items_than_targets_test() ->
    ?assertEqual(
        [{a, 1}, {b, 2}, {a, 3}, {b, 4}],
        % Because default_fold_fun/3 uses [X | Acc], the result is reversed.
        lists:reverse(
            kafine_round_robin:fold(fun default_fold_fun/3, [], [a, b], [1, 2, 3, 4])
        )
    ).

more_targets_than_items_test() ->
    ?assertEqual(
        [{a, 1}, {b, 2}],
        % Because default_fold_fun/3 uses [X | Acc], the result is reversed.
        lists:reverse(
            kafine_round_robin:fold(fun default_fold_fun/3, [], [a, b, c, d], [1, 2])
        )
    ).

skip_odd_items_test() ->
    % This is a contrived test, skipping some items.
    ?assertEqual(
        [2, 4, 6],
        lists:reverse(
            kafine_round_robin:fold(
                fun
                    (_Target, Item, Acc) when Item rem 2 == 0 -> {take, [Item | Acc]};
                    (_, _, _) -> skip
                end,
                [],
                [a, b],
                [1, 2, 3, 4, 5, 6]
            )
        )
    ).

skip_all_items_test() ->
    ?assertEqual(
        [],
        lists:reverse(
            kafine_round_robin:fold(fun(_, _, _) -> skip end, [], [a, b], [1, 2, 3, 4, 5, 6])
        )
    ).

fizz_buzz_test() ->
    % Not actual fizz-buzz (we only do "fizz" and "buzz", not "fizzbuzz"). It demonstrates filtering based on the target
    % _and_ the item (above, we only filtered on the item), and it uses a map as the accumulator, rather than a list.
    FizzBuzz = fun
        (fizz, N, Acc = #{fizz := Fizz}) when N rem 3 == 0 ->
            {take, Acc#{fizz => [N | Fizz]}};
        (buzz, N, Acc = #{buzz := Buzz}) when N rem 5 == 0 ->
            {take, Acc#{buzz => [N | Buzz]}};
        (_, _, _) ->
            skip
    end,
    ?assertEqual(
        #{
            fizz => [18, 12, 9, 6, 3],
            buzz => [20, 15, 10, 5]
        },
        kafine_round_robin:fold(FizzBuzz, #{fizz => [], buzz => []}, [fizz, buzz], lists:seq(1, 20))
    ).

targets_repeats_test() ->
    T = kafine_round_robin:new([a, b, c, d]),
    [a, b, c, d] = kafine_round_robin:to_list(T),
    {{value, a}, T2} = kafine_round_robin:out(T),
    {{value, b}, T3} = kafine_round_robin:out(T2),
    [c, d, a, b] = kafine_round_robin:to_list(T3),
    {{value, c}, T4} = kafine_round_robin:out(T3),
    {{value, d}, T5} = kafine_round_robin:out(T4),
    {empty, T6} = kafine_round_robin:out(T5),
    % so far, it's just a queue, but then it repeats...
    {{value, a}, T7} = kafine_round_robin:out(T6),
    {{value, b}, T8} = kafine_round_robin:out(T7),
    {{value, c}, T9} = kafine_round_robin:out(T8),
    {{value, d}, T10} = kafine_round_robin:out(T9),
    [a, b, c, d] = kafine_round_robin:to_list(T10),
    {empty, _} = kafine_round_robin:out(T10),
    ok.
