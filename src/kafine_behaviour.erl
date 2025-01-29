-module(kafine_behaviour).
%%% Ensure that a behaviour module exports the required functions.
-moduledoc false.
-export([
    verify_callbacks_exported/2
]).

verify_callbacks_exported(Behaviour, Module) when is_atom(Behaviour), is_atom(Module) ->
    ensure_module_loaded(Behaviour),
    ensure_module_loaded(Module),

    case erlang:function_exported(Behaviour, behaviour_info, 1) of
        true ->
            Callbacks = apply(Behaviour, behaviour_info, [callbacks]),
            OptionalCallbacks = apply(Behaviour, behaviour_info, [optional_callbacks]),
            RequiredCallbacks = Callbacks -- OptionalCallbacks,
            verify_callbacks_exported(Behaviour, Module, RequiredCallbacks);
        _ ->
            error({not_a_behaviour, Behaviour})
    end;
verify_callbacks_exported(Behaviour, Module) ->
    erlang:error(badarg, [Behaviour, Module]).

verify_callbacks_exported(Behaviour, Module, RequiredCallbacks) ->
    Exports = apply(Module, module_info, [exports]),
    case RequiredCallbacks -- Exports of
        [] ->
            ok;
        MissingCallbacks ->
            erlang:error(
                {not_exported, #{
                    behaviour => Behaviour, module => Module, missing_callbacks => MissingCallbacks
                }}
            )
    end.

ensure_module_loaded(Module) ->
    case erlang:module_loaded(Module) of
        true ->
            ok;
        false ->
            case code:ensure_loaded(Module) of
                {module, Module} ->
                    ok;
                {error, _} ->
                    erlang:error({not_loaded, Module})
            end
    end.
