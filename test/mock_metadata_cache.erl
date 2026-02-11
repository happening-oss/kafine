-module(mock_metadata_cache).

-include_lib("eunit/include/eunit.hrl").

-export([expect/3]).

expect(Ref, Topics, Partitions) ->
    meck:expect(
        kafine_metadata_cache,
        partitions,
        fun(R, Ts) when R =:= Ref ->
            ?assertEqual([], Ts -- Topics),

            Metadata = #{leader => 101, replicas => [101], isr => [101]},
            #{T => #{P => Metadata || P <- Partitions} || T <- Ts}
        end
    ).
