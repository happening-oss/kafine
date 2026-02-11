# kafine

Kafine is a Kafka client for Erlang.

Kafine aims to achieve better performance and compatibility than existing Kafka client libraries in the BEAM ecosystem.
It does this by using the automatically-generated message codecs in the `kafcod` project.

While Kafine is all-new, and still a bit rough around the edges, it’s based on years of experience using Kafka in
production with our existing internal Kafka libraries.

## Stability

Note that this release is currently considered unstable. We’re using it in production, but only for a small number of
non-critical workloads. We’re working towards being production ready, and we’d appreciate feedback from the community.

## Examples

The following examples assume that you've got a Kafka broker (or cluster) running at `localhost:9092`.

If you want to use docker compose, there's a Kafka cluster (using ZooKeeper) defined in the `docker` directory; see the
`README.md` file in that directory for details.

Start the Erlang shell.

```sh
rebar3 shell
```

## Example: Topic consumer

```erlang
Ref = cars.
Bootstrap = #{host => "localhost", port => 9092}.
ConnectionOptions = #{}.
ConsumerOptions = #{}.
SubscriptionOptions = #{}.
Callback = {kafine_consumer_callback_logger, undefined}.
Topics = [<<"cars">>].
TopicOptions = #{<<"cars">> => #{offset_reset_policy => latest}}.
Metadata = #{}.

{ok, _} = kafine:start_topic_consumer(Ref, Bootstrap, ConnectionOptions, ConsumerOptions, SubscriptionOptions, Callback, Topics, TopicOptions, Metadata).
```

You can use (e.g.) `kcat` to produce a message:

```sh
echo "key=value" | kcat -P -b localhost:9092 -t cars -p 0 -K =
```

...and it will be logged by the callback:

```
2025-01-28T12:55:13.060444+00:00 [info] <0.864.0> (node 103) kafine_consumer_callback_logger:handle_record/4:42: cars:0#8 1738068913040 :: key = <<"value">> ([])
2025-01-28T12:55:13.060535+00:00 [info] <0.864.0> (node 103) kafine_consumer_callback_logger:report_parity/3:67: Reached end of topic cars [0] at offset 9
```

## Example: Group consumer

```erlang
Ref = cars.
Bootstrap = #{host => "localhost", port => 9092}.
ConnectionOptions = #{}.
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

## Example: Multiple group consumers

Kafine supports multiple group consumers in the same application. That's probably not very useful in practice, but it
makes for a nice demo:

```erlang
Ref1 = make_ref().
Ref2 = make_ref().
{ok, _} = kafine:start_group_consumer(
                Ref1,
                Bootstrap,
                ConnectionOptions,
                GroupId,
                MembershipOptions,
                ConsumerOptions,
                ConsumerCallback,
                Topics,
                TopicOptions
).
{ok, _} = kafine:start_group_consumer(
                Ref2,
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

## Example: Producer

```erlang
Ref = example.
Bootstrap = #{host => "localhost", port => 9092}.
ConnectionOptions = #{}.
Topic = <<"cars">>.
Partition = 0.

{ok, Pid} = kafine:start_producer(Ref, Bootstrap, ConnectionOptions).

Messages = [
    #{
        key => <<"key">>,
        value => <<"value">>,
        headers => []
    }
].
{ok, _} = kafine_producer:produce(Pid, Topic, Partition, #{}, Messages).
```
