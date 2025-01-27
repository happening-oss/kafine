-module(kafine_options).
-export([
    validate_options/4,
    validate_options/5
]).

validate_options(Options, Defaults, RequiredKeys, Strict) ->
    validate_options(Options, Defaults, RequiredKeys, Strict, fun(_Key, _Value) -> ok end).

validate_options(Options, Defaults, RequiredKeys, Strict, ValidateFun) ->
    Options2 = maps:merge(Defaults, Options),
    case RequiredKeys -- maps:keys(Options2) of
        [] -> ok;
        Missing -> error({missing_options, Missing})
    end,
    case {Strict, maps:keys(Options2) -- RequiredKeys} of
        {false, _} -> ok;
        {true, []} -> ok;
        {true, Extra} -> error({unexpected_options, Extra})
    end,
    maps:foreach(ValidateFun, Options2),
    Options2.
