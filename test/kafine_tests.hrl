-define(skip(Fun), {setup, fun() -> error(skipped) end, [Fun]}).
% -define(skip(Fun), fun() -> (fun(true) -> Fun; (_) -> skip end)(false) end).
