-module(kafine_topic_subscriber_options).
-export([
    validate_options/1
]).

validate_options(Options) ->
    kafine_options:validate_options(
        Options,
        #{},
        [
            subscription_callback,
            assignment_callback
        ],
        true,
        fun validate_option/2
    ).

validate_option(subscription_callback, {Module, _Args}) ->
    ok = kafine_behaviour:verify_callbacks_exported(kafine_subscription_callback, Module);
validate_option(assignment_callback, {Module, _Args}) ->
    ok = kafine_behaviour:verify_callbacks_exported(kafine_assignment_callback, Module).
