-define(assertReceived(Pattern), begin
    ((fun() ->
        TimeoutMs = 1_000,
        receive
            Pattern ->
                ok
        after TimeoutMs ->
            Messages = assert_received:flush(),
            erlang:error(
                {assertReceived, [
                    {module, ?MODULE},
                    {line, ?LINE},
                    {messages, Messages}
                ]}
            )
        end
    end)())
end).
