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
---
## Kafine Shell

`kafine_shell` exists to allow shell debugging of telemetry events.

The below snippet starts `kafine_shell`, sets the `logger` level and then uses the group consumer example from the main `readme`.

```sh
-> % rebar3 shell
===> Verifying dependencies...
===> Analyzing applications...
===> Compiling kafine
Erlang/OTP 27 [erts-15.2.1] [source] [64-bit] [smp:10:10] [ds:10:10:10] [async-threads:1] [jit]

Eshell V15.2.1 (press Ctrl+G to abort, type help(). for help)
===> Booted crc32cer
===> Booted snappyer
===> Booted telemetry
===> Booted kafcod
===> Booted gproc
===> Booted kafine
1> kafine_shell:start().
ok
2> logger:set_application_level(kafine, debug).
ok
3> Ref = cars.
cars
4> Bootstrap = #{host => "localhost", port => 9092}.
#{port => 9092,host => "localhost"}
5> ConnectionOptions = #{}.
#{}
6> GroupId = <<"group">>.
<<"group">>
7> MembershipOptions = #{}.
#{}
8> ConsumerOptions = #{}.
#{}
9> ConsumerCallback = {kafine_consumer_callback_logger, undefined}.
{kafine_consumer_callback_logger,undefined}
10> Topics = [<<"cars">>].
[<<"cars">>]
11> TopicOptions = #{<<"cars">> => #{}}.
#{<<"cars">> => #{}}
12>
    {ok, _} = kafine:start_group_consumer(
                    Ref,
                    Bootstrap,
                    ConnectionOptions,
                    GroupId,
                    MembershipOptions,
                    ConsumerOptions,
                    ConsumerCallback,
                    Topics,
                    TopicOptions
    ).
{ok,<0.1227.0>}
2025-02-17T13:08:40.793583+00:00 [debug] <0.1229.0> kafine_shell:telemetry_handler/4:52: [kafine,connection,request,start]: #{monotonic_time => -576460635718812930,system_time => 1739797720793576362} (#{port => 9092,host => <<"localhost">>,api_key => 10,group_id => <<"group">>,api_version => 3,telemetry_span_context => #Ref<0.1969061018.2435579905.96296>})
```
---
## Enriching Kafine Telemetry Events

There are multiple ways to enrich Kafine's telemetry events with your own metadata.

### Adding metadata to Connection Options

`kafine_connection` takes `ConnectionOptions` which allows clients to add metadata that gets attached to telemetry events that are fired from within `kafine_connection`.

`ConnectionOptions` is passed down to `kafine_connection` from most high level api's such as:
`kafine:start_topic_consumer/8`, `kafine:start_group_consumer/9`, `kafine_eager_rebalance:start_link/6` etc.

Note: if an event is not fired from `kafine_connection` i.e `[kafine, rebalance, triggered]` adding to `ConnectionOptions` won't yield any result.

```erlang

Ref = cars.
Bootstrap = #{host => "localhost", port => 9092}.
% Customise your connection options here
ConnectionOptions = #{something_important => <<"hello world">>}.
GroupId = <<"group">>.
MembershipOptions = #{}.
ConsumerOptions = #{}.
ConsumerCallback = {kafine_consumer_callback_logger, undefined}.
Topics = [<<"cars">>].
TopicOptions = #{<<"cars">> => #{}}.

{ok, _} = kafine:start_group_consumer(
                Ref,
                Bootstrap,
                ConnectionOptions,
                GroupId,
                MembershipOptions,
                ConsumerOptions,
                ConsumerCallback,
                Topics,
                TopicOptions
).

```

Your custom metadata is now accessible via:

```erlang
    ok = telemetry:attach_many(
        my_handler,
        [
            [kafine, fetch, partition_data]
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
        something_important := _ImportantMetadata
    },
    _Config
) ->
    % Fire metrics with your favourite metrics library
    ok.
```
### Span Enrichment

All of Kafine's communications with Kafka Brokers are instrumented with `telemetry`'s spans. The main events are:

```erlang
[
    [kafine, connection, request, start],
    [kafine, connection, call, start],
    [kafine, connection, request, stop],
    [kafine, connection, call, stop],
    [kafine, connection, request, exception],
    [kafine, connection, call, exception]
]
```
Kafine allows the configuration of a `span_enrichment_module`. This callback module gives clients an opportunity to add labels to the span's metadata before it gets fired by `telemetry`. Kafine will invoke the given module's `enrich_span/2` expecting a `telemetry:span` return type.

Note: More specifically, only when we attempt to `stop` the span that the `span_enrichment_module` is invoked. The enrichment module is *NOT* invoked on span `start` and `exception`. See `kafine_connection_telemetry` for more information.

It is configurable via the application environment (defaults to `undefined`) as such:

`*.erl`
```erlang
ok = application:set_env(kafine, span_enrichment_module, my_span_enrichment_module),
```

or `*.config`
```erlang
    {kafine, [
        {span_enrichment_module, my_span_enrichment_module}
    ]},
```
We have found particular use for this when trying to emit metrics that expose nested error codes within responses with topic partition data.

Example `span_enrichment_module`:

```erlang
-module(error_code_enrichment).

-export([
    enrich_span/2
]).
enrich_span(Response, {Measurements, Metadata} = _Span) ->
    % This fun needs to return {Measurements, Metadata} A.K.A a span
    {Measurements, find_errors(Response, Metadata)}.

find_errors(Response, Metadata) ->
    % Find errors and attach to metadata
    Metadata.
```
After instrumenting the spans with custom metadata, you can then access it via the `Labels` arg as seen in the telemetry handler examples above.
