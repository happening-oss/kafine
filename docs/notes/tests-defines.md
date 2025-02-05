# What's going on with the `-define` directives in the unit tests?

At the top of many of the unit tests, you'll find a collection of `-define` directives, similar to this:

```erlang
-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s_g", [?MODULE, ?FUNCTION_NAME]))).
-define(PARTITION_1, 61).
-define(PARTITION_2, 62).
-define(CONSUMER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CALLBACK_ARGS, undefined).
-define(CALLBACK_STATE, ?MODULE).
-define(WAIT_TIMEOUT_MS, 2_000).
-define(CONNECTION_OPTIONS, #{}).
```

Some of them are to explain "magic" values. Examples of this are `?PARTITION_1`, `?CONNECTION_OPTIONS`, etc.

The following are less obvious:

```erlang
-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(CALLBACK_STATE, ?MODULE).
```

These are for when the test fails. When the test fails with an error report, it often contains the state of the involved
processes. By using the module name (and sometimes the function name) as the broker reference or the consumer callback
state, there's a fair chance that they'll appear in the error report. This makes it easier to figure out which test is
failing.

Similarly, there's `?TOPIC_NAME` and `?GROUP_ID`. Here's `?TOPIC_NAME`:

```erlang
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s_t", [?MODULE, ?FUNCTION_NAME]))).
```

Because this is a topic name, it needs to be a binary string, rather than an Erlang term. This means that we have to use
`iolist_to_binary(io_lib:format(...))`.

The trailing `_t` is so that you can see that it's a topic name. For consumer groups, we use a trailing `_g`.

This makes it slightly less confusing when you see the group name and topic name together -- if we used the same for
both (no suffix), you might think the code accidentally passed the topic name twice, for example.
