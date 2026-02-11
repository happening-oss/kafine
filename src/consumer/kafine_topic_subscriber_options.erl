-module(kafine_topic_subscriber_options).
-export([
    validate_options/1
]).

-export_type([t/0]).

-type t() :: #{
    subscription_callback := {module(), term()},
    assignment_callback := {module(), term()}
}.

-spec validate_options(map()) -> t().

validate_options(Options) ->
    kafine_options:validate_options(
        Options,
        default_options(),
        required_options(),
        true,
        fun validate_option/2
    ).

default_options() ->
    #{}.

required_options() ->
    [subscription_callback, assignment_callback].

validate_option(subscription_callback, {Module, _Args}) ->
    ok = kafine_behaviour:verify_callbacks_exported(kafine_subscription_callback, Module);
validate_option(assignment_callback, {Module, _Args}) ->
    ok = kafine_behaviour:verify_callbacks_exported(kafine_assignment_callback, Module).
