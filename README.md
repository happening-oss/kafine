# kafine

Kafine is "Kafire vNext". Kafire is our original Kafka Wire Protocol implementation.

## Example: Simple topic consumer

```erlang
Ref = cars.
Bootstrap = #{host => "localhost", port => 9092}.
ConnectionOptions = #{client_id => <<"topic_consumer">>}.
ConsumerOptions = #{}.
SubscriptionOptions = #{}.
Callback = {kafine_consumer_callback_logger, undefined}.
Topics = [<<"cars">>].
TopicOptions = #{<<"cars">> => #{offset_reset_policy => latest}}.

{ok, _} = kafine:start_topic_consumer(Ref, Bootstrap, ConnectionOptions, ConsumerOptions, SubscriptionOptions, Callback, Topics, TopicOptions).
```

`kafine_consumer_callback_logger` allows you to pass a formatting function. So, for example, if your messages are
binary-term-encoded, you might use this:

```erlang
Callback =
    {kafine_consumer_callback_logger, #{
        formatter =>
            fun(K, V, H) ->
                io_lib:format("~s = ~p (~p)", [K, binary_to_term(V), H])
            end
    }}.
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

## Example: Simple producer

```erlang
Ref = example.
Bootstrap = #{host => "localhost", port => 9092}.
ConnectionOptions = #{}.
Topic = <<"cars">>.
Partition = 0.

{ok, Pid} = kafine_producer:start_link(Ref, Bootstrap, ConnectionOptions).

Messages = [
    #{
        key => <<"key">>,
        value => <<"value">>,
        headers => []
    }
].
{ok, _} = kafine_producer:produce(Pid, Topic, Partition, #{}, Messages).
```
