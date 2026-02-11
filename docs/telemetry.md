# Telemetry and Metrics

Kafine uses `telemetry` to emit events that are operationally important. These events can be used to create metrics or perform diagnosis with.

## Attaching to Telemetry

In order to create metrics from Kafine's telemetry events, the first step is to create a telemetry handler and attach to the events you care about.

A Kafine event typically follows this naming convention: `[kafine, module, name_of_event]`.

All Kafine events will have some labels added to them, a few examples are listed below. Not all events have the same labels.

Note: `node_id` is added to all events that use `kafine_connection`.

```erlang
ok = telemetry:attach_many(
    my_handler,
    [
        [kafine, fetch, partition_data],
        [kafine, connection, request, stop],
        [kafine, connection, call, stop]
    ],
    fun ?MODULE:telemetry_handler/4,
    #{}
    ).

telemetry_handler(
    [kafine, fetch, partition_data] = _Event,
    #{
        fetch_offset := _,
        next_offset := _,
        high_watermark := _,
        lag := _
    } = _Measurements,
    #{
        node_id := _,
        topic := _,
        partition := _,
    },
    _Config
) ->
    % Fire metrics with your favourite metrics library
    ok;
telemetry_handler(
    [kafine, connection, request, stop] = _Event,
    _Measurements,
    Labels,
    _Config
) ->
    % Fire metrics with your favourite metrics library
    ok;
telemetry_handler(
    [kafine, connection, call, stop] = _Event,
    _Measurements,
    Labels,
    _Config
) ->
    % Fire metrics with your favourite metrics library
    ok.
```
