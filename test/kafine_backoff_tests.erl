-module(kafine_backoff_tests).

-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    [
        fun fixed_consistently_returns_same_backoff/0,
        fun exponential_returns_increasing_backoff/0,
        fun exponential_respects_max_ms/0,
        fun exponential_accepts_infinity_max_ms/0,
        fun exponential_respects_max_count/0
    ].

fixed_consistently_returns_same_backoff() ->
    IntervalMs = 123,
    Config = kafine_backoff:fixed(IntervalMs),
    State = kafine_backoff:init(Config),
    {IntervalMs, State1} = kafine_backoff:backoff(State),
    {IntervalMs, State2} = kafine_backoff:backoff(State1),
    {IntervalMs, State3} = kafine_backoff:backoff(State2),
    {IntervalMs, _State4} = kafine_backoff:backoff(State3).

exponential_returns_increasing_backoff() ->
    Initial = 10,
    Config = kafine_backoff:exponential(#{initial_ms => Initial}),
    State = kafine_backoff:init(Config),
    {10, State1} = kafine_backoff:backoff(State),
    {20, State2} = kafine_backoff:backoff(State1),
    {40, State3} = kafine_backoff:backoff(State2),
    {80, _State4} = kafine_backoff:backoff(State3).

exponential_respects_max_ms() ->
    Config = kafine_backoff:exponential(10, 30),
    State = kafine_backoff:init(Config),
    {10, State1} = kafine_backoff:backoff(State),
    {20, State2} = kafine_backoff:backoff(State1),
    {30, State3} = kafine_backoff:backoff(State2),
    {30, _State4} = kafine_backoff:backoff(State3).

exponential_accepts_infinity_max_ms() ->
    Config = kafine_backoff:exponential(600_000, infinity),
    State = kafine_backoff:init(Config),
    {600_000, State1} = kafine_backoff:backoff(State),
    {1_200_000, State2} = kafine_backoff:backoff(State1),
    {2_400_000, State3} = kafine_backoff:backoff(State2),
    {4_800_000, _State4} = kafine_backoff:backoff(State3).

exponential_respects_max_count() ->
    Initial = 10,
    Config = kafine_backoff:exponential(#{initial_ms => Initial, max_count => 2}),
    State = kafine_backoff:init(Config),
    {10, State1} = kafine_backoff:backoff(State),
    {20, State2} = kafine_backoff:backoff(State1),
    limit_exceeded = kafine_backoff:backoff(State2).
