# offset_reset_policy

When you fetch from a Kafka partition, you need to tell it which offset you want to fetch from. As you fetch more
records, you keep track of the most-recent offset, and you use it in the next fetch request.

If we try to fetch from a non-existent offset, Kafka will return an `OFFSET_OUT_OF_RANGE` error.

The same applies if you're a group consumer and you got your last-committed offset using `OffsetFetch` -- that message
might no longer exist.

So we need to perform a `ListOffsets` request. This allows you to specify a (rough) timestamp for the message you'd like
to resume fetching from. It also allows two special timestamp values: $-1$ for the latest message, which allows you to
only see new messages; and $-2$ for the earliest message, which allows you to start from the beginning.

## `offset_reset_policy` consumer option

In Kafine, this behaviour is controlled by the `offset_reset_policy` consumer option. It can be set to `earliest` (the
default), or `latest`.

The `earliest` and `latest` values correspond to the two magic values above. Then, if Kafine receives an
`OFFSET_OUT_OF_RANGE` error, it will discover (reset) the offset according to this option.
