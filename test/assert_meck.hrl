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

-define(assertWait(Mod, OptFun, OptArgsSpec, Timeout), ?assertWait(1, Mod, OptFun, OptArgsSpec, Timeout)).

-define(assertWait(Count, Mod, OptFun, OptArgsSpec, Timeout), begin
    ((fun() ->
        X__X = (catch meck:wait(Count, Mod, OptFun, OptArgsSpec, Timeout)),
        case (X__X) of
            ok ->
                ok;
            {'EXIT', {timeout, _}} ->
                erlang:error(
                    {assertWait, [
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
