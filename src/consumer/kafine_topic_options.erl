-module(kafine_topic_options).
-export([
    validate_options/2,
    validate_options/1,

    merge_options/2
]).

-define(DEFAULT_INITIAL_OFFSET, earliest).
-define(DEFAULT_OFFSET_RESET_POLICY, latest).

validate_options(Topics, TopicOptions) ->
    % for each listed topic, make sure it appears in the TopicOptions.
    [] = maps:keys(TopicOptions) -- Topics,
    % for each topic in TopicOptions, make sure the options exist, with defaults.
    lists:foldl(
        fun(TopicName, Acc) when is_binary(TopicName) ->
            Options0 = maps:get(TopicName, TopicOptions, #{}),
            Options = validate_options(Options0),
            Acc#{TopicName => Options}
        end,
        TopicOptions,
        Topics
    ).

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
        initial_offset => ?DEFAULT_INITIAL_OFFSET,
        offset_reset_policy => ?DEFAULT_OFFSET_RESET_POLICY
    }.

required_options() ->
    [initial_offset, offset_reset_policy].

validate_option(initial_offset, Value) when Value == earliest; Value == latest ->
    ok;
validate_option(initial_offset, Value) when is_integer(Value) ->
    ok;
validate_option(offset_reset_policy, Value) when Value == earliest; Value == latest ->
    ok;
validate_option(Key, Value) ->
    error(badarg, [Key, Value]).

merge_options(TopicOptions0, TopicOptions1) ->
    CombineOptions = fun
        (TopicName, Options0, Options1) when Options0 /= Options1 ->
            error({incompatible_topic_options, TopicName, Options0, Options1});
        (_TopicName, Options0, _Options1) ->
            Options0
    end,
    maps:merge_with(CombineOptions, TopicOptions0, TopicOptions1).
