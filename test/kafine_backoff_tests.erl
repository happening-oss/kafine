-module(kafine_backoff_tests).

-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    [
        fun fixed_consistently_returns_same_backoff/0,
        fun fixed_reset_preserves_interval/0
    ].

fixed_consistently_returns_same_backoff() ->
    IntervalMs = 123,
    Config = kafine_backoff:fixed(IntervalMs),
    State = kafine_backoff:init(Config),
    {IntervalMs, State1} = kafine_backoff:backoff(State),
    {IntervalMs, State2} = kafine_backoff:backoff(State1),
    {IntervalMs, State3} = kafine_backoff:backoff(State2),
    {IntervalMs, _State4} = kafine_backoff:backoff(State3).

fixed_reset_preserves_interval() ->
    IntervalMs = 456,
    Config = kafine_backoff:fixed(IntervalMs),
    State = kafine_backoff:init(Config),
    {IntervalMs, State1} = kafine_backoff:backoff(State),
    State2 = kafine_backoff:reset(State1),
    {IntervalMs, _State3} = kafine_backoff:backoff(State2).
