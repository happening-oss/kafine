# Notes on manually interacting with a kafka coordinator from an erlang shell

## Basic usage:

Set useful variables:

```erlang
Bootstrap = #{port => 9092,host => "localhost"},
Ref = test,
Topic = <<"test">>,
Topics = [Topic],
GroupId = <<"test">>,
ProtocolName = <<"range">>,
COpts = kafine_connection_options:validate_options(#{}),
MOpts = kafine_membership_options:validate_options(#{
    assignment_callback => {kafine_noop_assignment_callback, undefined},
    subscription_callback => {kafine_parallel_subscription_callback, #{}},
    session_timeout_ms => 300_000,
    rebalance_timeout_ms => 30_000,
    heartbeat_interval_ms => 300_000
}),
WaitResp = fun(ReqIds) ->
    {{reply, Result}, _, _} = gen_statem:wait_response(ReqIds, 35_000, true),
    Result
end.
```

Create bootstrap and coordinator connections:

```erlang
{ok, B} = kafine_bootstrap:start_link(Ref, Bootstrap, COpts),
{ok, C} = kafine_coordinator:start_link(Ref, GroupId, Topics, COpts, MOpts).
```

Get member id:

```erlang
{error, {member_id_required, MemberId}} = WaitResp(kafine_coordinator:join_group(Ref, <<>>)).
```

Join group:

```erlang
{ok, #{generation_id := GenerationId}} = WaitResp(kafine_coordinator:join_group(Ref, MemberId)).
```

Sync group:

```erlang
{ok, _} = WaitResp(kafine_coordinator:sync_group(Ref, MemberId, GenerationId, ProtocolName, #{})).
```

Heartbeat:

```erlang
WaitResp(kafine_coordinator:heartbeat(Ref, MemberId, GenerationId)).
```

Offset commit:

```erlang
WaitResp(kafine_coordinator:offset_commit(Ref, MemberId, GenerationId, #{Topic => #{0 => 0}}, undefined, #{})).
```

Offset fetch:

```erlang
WaitResp(kafine_coordinator:offset_fetch(Ref, #{Topic => [0]}, #{})).
```

Leave group:

```erlang
WaitResp(kafine_coordinator:leave_group(Ref, MemberId)).
```

If something goes wrong and you need to start over, you can reset transient variables with:

```erlang
f(B),f(C),f(MemberId),f(GenerationId).
```

## How-tos

### Moving a coordinator

Identify the broker for your coordinator via broker.node_id from the coordinator info:

```
16> kafine_coordinator:info(Ref).
#{state => connected,connection => <0.517.0>,
  topics => [<<"test">>],
  broker =>
      #{port => 9094,host => <<"192.168.2.250">>,node_id => 103},
  connection_options =>
      #{metadata => #{},connect_timeout => infinity,
        client_id => <<"kafine">>,request_timeout_ms => 30000,
        transport => gen_tcp,transport_options => []},
  pending_requests => [],group_id => <<"test">>,
  membership_options =>
      #{assignment_callback =>
            {kafine_noop_assignment_callback,undefined},
        subscription_callback =>
            {kafine_parallel_subscription_callback,#{}},
        session_timeout_ms => 300000,rebalance_timeout_ms => 30000,
        heartbeat_interval_ms => 300000,
        assignors => [kafine_range_assignor]}}
```

exec into the broker shell:

```bash
docker exec -it kafka-default-103 bash
```

Find the `__consumer_offsets` partition for your consumer group using jshell:

```
❯ jshell
|  Welcome to JShell -- Version 25
|  For an introduction type: /help intro

jshell> Math.abs("test".hashCode()) % 50
$1 ==> 48
```

Create a reassignment file:

```bash
cat > reass.json <<EOF
{
    "partitions":
    [{"topic": "__consumer_offsets",
        "partition": 48,
        "replicas": [102,101,103]
    }],
    "version":1
}
EOF
```

Run the reassignment:

```bash
kafka-reassign-partitions --bootstrap-server localhost:9094 --reassignment-json-file reass.json --execute
```

Verify reassignment has completed:

```bash
$ kafka-reassign-partitions --bootstrap-server localhost:9094 --reassignment-json-file reass.json --verify
Status of partition reassignment:
Reassignment of partition __consumer_offsets-48 is completed.
```

Force a leader election to ensure the leader moves:

```bash
kafka-leader-election --bootstrap-server localhost:9094 --election-type preferred --all-topic-partitions
```
