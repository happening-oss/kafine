-define(assertCalled(Mod, OptFun, OptArgsSpec), begin
    ((fun() ->
        X__X = (meck:called(Mod, OptFun, OptArgsSpec)),
        case (X__X) of
            true ->
                ok;
            false ->
                erlang:error(
                    {assertCalled, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {expected, {Mod, OptFun, ??OptArgsSpec}},
                        {value,
                            lists:filtermap(
                                fun
                                    ({_, {Mod, X__Fun, _}, _}) when
                                        X__Fun =:= OptFun; OptFun =:= '_'
                                    ->
                                        true;
                                    (_) ->
                                        false
                                end,
                                meck:history(Mod)
                            )}
                    ]}
                )
        end
    end)())
end).

-define(assertNotCalled(Mod, OptFun, OptArgsSpec), begin
    ((fun() ->
        X__X = (meck:called(Mod, OptFun, OptArgsSpec)),
        case (X__X) of
            false ->
                ok;
            true ->
                erlang:error(
                    {assertNotCalled, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {expected, {Mod, OptFun, ??OptArgsSpec}},
                        {value,
                            lists:filtermap(
                                fun
                                    ({_, {Mod, X__Fun, _}, _}) when
                                        X__Fun =:= OptFun; OptFun =:= '_'
                                    ->
                                        true;
                                    (_) ->
                                        false
                                end,
                                meck:history(Mod)
                            )}
                    ]}
                )
        end
    end)())
end).

-define(assertWait(Mod, OptFun, OptArgsSpec, Timeout),
    ?assertWait(1, Mod, OptFun, OptArgsSpec, Timeout)
).

-define(assertWait(Count, Mod, OptFun, OptArgsSpec, Timeout), begin
    ((fun() ->
        try (meck:wait(Count, Mod, OptFun, OptArgsSpec, Timeout)) of
            _X__V -> ok
        catch
            error:timeout:_X__S ->
                erlang:error(
                    {assertWait, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {expected, {Count, Mod, OptFun, ??OptArgsSpec}},
                        {value,
                            lists:filtermap(
                                fun
                                    ({_, {Mod, X__Fun, _}, _}) when
                                        X__Fun =:= OptFun; OptFun =:= '_'
                                    ->
                                        true;
                                    (_) ->
                                        false
                                end,
                                meck:history(Mod)
                            )}
                    ]}
                )
        end
    end)())
end).
