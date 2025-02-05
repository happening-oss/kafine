-module(kafine_logger_filters).
-include("kafine_doc.hrl").

?MODULEDOC("""
Filters to use with Logger.

When the connection to the broker is dropped, Erlang reports this as an error. By using this logger filter, you can
stop (or downgrade) the logging.
""").

-export([connection_errors/2]).

?DOC("""
This filter provides a way to suppress or downgrade errors caused by `kafine_connection` crashing.

Since kafine reconnects automatically, these reports are just noise in the logs.

## Examples

Put it in your `sys.config` as follows:

```erlang
{kernel, [
  {logger, [
    {filters, log, [
      % Stop {exit, closed} errors from kafine_connection.
      % If, instead of 'stop', you specify 'warn', the errors are downgraded to warnings.
      {stop_connection_errors, {fun kafine_logger_filters:connection_errors/2, stop}}
      % ...
```

Or you can add the filter programmatically:

```erlang
ok = logger:add_primary_filter(
    stop_connection_errors, {fun kafine_logger_filters:connection_errors/2, stop}
),
```
""").
-spec connection_errors(logger:log_event(), Options :: warn | stop) -> logger:filter_return().
connection_errors(
    LogEvent = #{
        level := error,
        msg := {report, #{modules := [kafine_connection], reason := {exit, closed, _}}}
    },
    Options
) ->
    apply_filter(LogEvent, Options);
connection_errors(
    LogEvent = #{
        level := error,
        msg := {report, #{label := {proc_lib, crash}, report := [Info, []]}}
    },
    Options
) ->
    case proplists:get_value(initial_call, Info) of
        {kafine_connection, init, _} ->
            apply_filter(LogEvent, Options);
        _ ->
            ignore
    end;
connection_errors(_LogEvent, _Options) ->
    ignore.

apply_filter(LogEvent, _Options = warn) ->
    LogEvent#{level := warning};
apply_filter(_LogEvent, _Options = stop) ->
    stop;
apply_filter(_LogEvent, _Options) ->
    ignore.
