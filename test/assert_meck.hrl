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
