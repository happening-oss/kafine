-module(kafine_behaviour).
%%% Ensure that a behaviour module exports the required functions.
-moduledoc false.
-export([
    verify_callbacks_exported/2
]).

verify_callbacks_exported(Behaviour, Module) when is_atom(Behaviour), is_atom(Module) ->
    % Load the specified module and behaviour.
    ensure_module_loaded(Behaviour),
    ensure_module_loaded(Module),

    % Is it a behaviour? (does it export behaviour_info/1?)
    case erlang:function_exported(Behaviour, behaviour_info, 1) of
        true ->
            % Work out the required callbacks by subtracting the optional callbacks from the list of all callbacks.
            AllCallbacks = apply(Behaviour, behaviour_info, [callbacks]),
            OptionalCallbacks = apply(Behaviour, behaviour_info, [optional_callbacks]),
            RequiredCallbacks = AllCallbacks -- OptionalCallbacks,

            % Are those callbacks exported?
            verify_callbacks_exported(Behaviour, Module, RequiredCallbacks);
        _ ->
            % Doesn't export behaviour_info/1; it's not a behaviour.
            error({not_a_behaviour, Behaviour})
    end;
verify_callbacks_exported(Behaviour, Module) ->
    erlang:error(badarg, [Behaviour, Module]).

verify_callbacks_exported(Behaviour, Module, RequiredCallbacks) ->
    % What functions are exported?
    Exports = apply(Module, module_info, [exports]),
    verify_callbacks_exported(Behaviour, Module, RequiredCallbacks, Exports).

verify_callbacks_exported(Behaviour, Module, RequiredCallbacks, Exports) ->
    % If there are required callbacks that aren't in this list, they're missing.
    case RequiredCallbacks -- Exports of
        [] ->
            % All of the required callbacks appear in the exports; we're good.
            ok;
        MissingCallbacks ->
            % There are callbacks missing; report an error.
            erlang:error(
                {not_exported, #{
                    behaviour => Behaviour, module => Module, missing_callbacks => MissingCallbacks
                }}
            )
    end.

ensure_module_loaded(Module) ->
    case erlang:module_loaded(Module) of
        true ->
            % Module is already loaded.
            ok;
        false ->
            % Try to load the module.
            case code:ensure_loaded(Module) of
                {module, Module} ->
                    ok;
                {error, _} ->
                    erlang:error({not_loaded, Module})
            end
    end.
