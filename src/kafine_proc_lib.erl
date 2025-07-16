-module(kafine_proc_lib).
-moduledoc false.
-export([set_label/1]).
-export([get_dictionary/3]).

%%% Wrapper for `proc_lib:set_label/1`, which requires OTP-27.
set_label(Label) ->
    % proc_lib:set_label requires OTP-27.x...
    set_label(erlang:function_exported(proc_lib, set_label, 1), Label).

% ...and dialyzer (when run on OTP-26.x) doesn't know it exists.
-dialyzer({nowarn_function, set_label/2}).

set_label(false, _Label) ->
    ok;
set_label(true, Label) ->
    proc_lib:set_label(Label).

%%% Wrapper for `process_info(Pid, {dictionary, Key})`, which requires OTP-26.2
get_dictionary(Pid, Key, Default) ->
    try
        case process_info(Pid, {dictionary, Key}) of
            {_, Metadata} -> Metadata;
            _ -> Default
        end
    catch
        error:badarg ->
            {dictionary, Dict} = process_info(Pid, dictionary),
            proplists:get_value(Key, Dict, Default)
    end.
