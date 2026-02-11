-module(kafine_parallel_subscription_callback).

-behaviour(kafine_subscription_callback).

-export([validate_options/1]).

-export([
    init/1,
    subscribe_partitions/3,
    unsubscribe_partitions/1
]).

-export_type([options/0]).

-type options() :: #{
    topic_options := #{kafine:topic() => kafine:topic_options()},
    callback_mod := module(),
    callback_arg := term(),
    offset_callback := module(),
    skip_empty_fetches := boolean() | after_first
}.

validate_options(Options) ->
    kafine_options:validate_options(
        Options,
        default_options(),
        required_options(),
        true,
        fun validate_option/2
    ).

default_options() ->
    #{
        offset_callback => kafine_group_consumer_offset_callback,
        skip_empty_fetches => true,
        error_mode => reset
    }.

required_options() ->
    [
        topic_options,
        callback_mod,
        callback_arg,
        offset_callback,
        skip_empty_fetches,
        error_mode
    ].

validate_option(topic_options, TopicOptions) ->
    kafine_topic_options:validate_options(maps:keys(TopicOptions), TopicOptions);
validate_option(callback_mod, CallbackMod) ->
    ok = kafine_behaviour:verify_callbacks_exported(kafine_consumer_callback, CallbackMod);
validate_option(callback_arg, _) ->
    ok;
validate_option(offset_callback, OffsetCallback) ->
    ok = kafine_behaviour:verify_callbacks_exported(
        kafine_adjust_fetched_offset_callback, OffsetCallback
    );
validate_option(skip_empty_fetches, SkipEmptyFetches) ->
    case SkipEmptyFetches of
        true -> ok;
        false -> ok;
        after_first -> ok;
        _ -> error(badarg, [skip_empty_fetches, SkipEmptyFetches])
    end;
validate_option(error_mode, ErrorMode) ->
    case ErrorMode of
        reset -> ok;
        retry -> ok;
        skip -> ok;
        _ -> error(badarg, [error_mode, ErrorMode])
    end;
validate_option(Key, Value) ->
    error(badarg, [Key, Value]).

init(Ref) ->
    % Link the subscriber to the callback pid. If the callback dies it needs a resubscribe, and if
    % the subscriber dies it'll be expecting a fresh callback when it restarts.
    case kafine_parallel_subscription_impl:whereis(Ref) of
        undefined ->
            error(noproc);
        Pid ->
            link(Pid),
            {ok, Ref}
    end.

subscribe_partitions(_Coordinator, Assignment, State) ->
    kafine_parallel_subscription_impl:subscribe_partitions(State, Assignment),
    {ok, State}.

unsubscribe_partitions(State) ->
    kafine_parallel_subscription_impl:unsubscribe_all(State),
    {ok, State}.
