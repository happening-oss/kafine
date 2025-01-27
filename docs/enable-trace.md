# gen_statem tracing

All of the `gen_statem` implementations in kafine hook the tracing up to the Erlang logger. See the blog posts linked
below for details.

It's disabled by default, for performance reasons. To enable it, turn on `enable_trace`:

```erlang
    {kafine, [
        {enable_trace, true}
    ]},

    % ...
```

Logging will use `debug` level. To make sure it's displayed, you can set the global log level to `debug`, as follows:

```erlang
    {kernel, [
        {logger_level, debug},
        % ...
```

This is exceptionally noisy. Consider, e.g., disabling logging for `kafine_connection`:

```erlang
    {kernel, [
        {logger_level, debug},

        {logger, [
            {module_level, none, [kafine_connection]},
            % ...
```

## Links

- <https://blog.differentpla.net/blog/2024/05/20/erlang-gen-server-redirect-debug-trace-to-logger/>
- <https://blog.differentpla.net/blog/2023/05/10/erlang-gen-server-debug/>
