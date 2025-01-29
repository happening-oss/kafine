-module(kafine_proc_lib).
-moduledoc false.
%%% Wrapper for proc_lib:set_label/1 (which requires OTP-27).
-export([set_label/1]).

set_label(Label) ->
    % proc_lib:set_label requires OTP-27.x...
    set_label(erlang:function_exported(proc_lib, set_label, 1), Label).

% ...and dialyzer (when run on OTP-26.x) doesn't know it exists.
-dialyzer({nowarn_function, set_label/2}).

set_label(false, _Label) ->
    ok;
set_label(true, Label) ->
    proc_lib:set_label(Label).
