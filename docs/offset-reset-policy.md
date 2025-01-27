# offset_reset_policy

## Background

When you fetch from a Kafka partition, you need to tell it which offset you want to fetch from. As you fetch more
records, you keep track of the most-recent offset, and you use it in the next fetch request.

However, if you want to start from the beginning of the topic (the earliest messages), you can't just start at offset
zero, because it might have been deleted. Similarly, if you want to start from the end of the topic (the latest
messages), you can't just guess.

If we try to fetch from a non-existent offset, Kafka will return an `OFFSET_OUT_OF_RANGE` error.

The same applies if you're a group consumer and you got your last-committed offset using `OffsetFetch` -- that message
might no longer exist.

So we need to perform a `ListOffsets` request. This allows you to specify a (rough) timestamp for the message you'd like
to resume fetching from. It also allows two special timestamp values: $-1$ for the latest message, which allows you to
only see new messages; and $-2$ for the earliest message, which allows you to start from the beginning.

## `offset_reset_policy` consumer option

In Kafine, this behaviour is controlled by the `offset_reset_policy` consumer option. It can be set to `earliest` (the
default), or `latest`. or a module name.

The `earliest` and `latest` values correspond to the two magic values above. Then, if Kafine receives an
`OFFSET_OUT_OF_RANGE` error, it will discover (reset) the offset according to this option.

If you specify a module, it must implement the `kafine_offset_reset_policy` behaviour. You can see an examples
of this in the `kafine_last_offset_reset_policy` module.

## `kafine_offset_reset_policy` behaviour

When a fetch fails with `OFFSET_OUT_OF_RANGE`, kafine calls `OffsetResetPolicy:timestamp()`, which should return either
`?EARLIEST_TIMESTAMP` or `?LATEST_TIMESTAMP`. This value is passed as the timestamp value in `ListOffsets`.

Kafine doesn't currently support returning an arbitrary timestamp from `OffsetResetPolicy:timestamp()`.

`OffsetResetPolicy:adjust_offset()` takes two parameters.

The first, `LastOffsetFetched`, is the offset that we used in the failing fetch request. For a simple topic consumer, for
a first fetch, this will be `-1`. For a group consumer, for a first fetch, this will be the committed offset (returned
from `OffsetFetch`).

The second, `NextOffsetToFetch`, is the offset returned from the `ListOffsets` request.
