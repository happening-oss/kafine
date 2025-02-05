-module(kafine_shell).
-moduledoc false.

-export([start/0]).
-export([attach_telemetry/0]).
-export([telemetry_handler/4]).

-include_lib("kernel/include/logger.hrl").

-define(APPLICATION, kafine).

start() ->
    {ok, _} = application:ensure_all_started(?APPLICATION),
    attach_telemetry(),
    ok.

attach_telemetry() ->
    % Output telemetry to the logger; useful for ad-hoc testing.
    ok = telemetry:attach_many(
        <<"telemetry">>,
        [
            [kafcod, encode],
            [kafcod, decode],

            [kafcod, records, decode_message_set],
            [kafcod, records, encode_message_set],

            [kafine, connection, request, start],
            [kafine, connection, request, stop],
            [kafine, connection, request, exception],

            [kafine, connection, call, start],
            [kafine, connection, call, stop],
            [kafine, connection, call, exception],

            [kafine, connection, tcp, bytes_sent],
            [kafine, connection, tcp, bytes_received],

            [kafine, fetch, partition_data],

            [kafine, rebalance, join_group],
            [kafine, rebalance, leader],
            [kafine, rebalance, follower],

            [kafine, consumer, subscription]
        ],
        fun ?MODULE:telemetry_handler/4,
        []
    ).

telemetry_handler(Event, Metrics, Labels, _Config = []) ->
    ?LOG_DEBUG("~p: ~p (~p)", [Event, Metrics, Labels]),
    ok.
