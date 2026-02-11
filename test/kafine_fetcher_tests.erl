-module(kafine_fetcher_tests).

-include_lib("eunit/include/eunit.hrl").
-include("assert_meck.hrl").
-include("assert_received.hrl").
% -include_lib("kernel/include/logger.hrl").

-define(REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s_t", [?MODULE]))).
-define(TOPIC_NAME_2, <<(?TOPIC_NAME)/binary, "_2">>).
-define(CALLBACK_ARGS, undefined).
-define(BROKERS, [#{node_id => 101}, #{node_id => 102}, #{node_id => 103}]).
-define(FETCHER_METADATA, #{}).
-define(WAIT_TIMEOUT_MS, 2_000).

% -compile(nowarn_unused_function).

setup() ->
    meck:new(kafine_metadata_cache, [non_strict, stub_all]),

    meck:new(kafine_node_fetcher_sup, [stub_all]),
    meck:expect(kafine_node_fetcher_sup, start_child, fun(_, _, _, _) -> {ok, self()} end),

    meck:new(kafine_node_fetcher, []),
    meck:expect(kafine_node_fetcher, job, fun(_, _) -> ok end),

    ok.

cleanup(_) ->
    meck:unload().

kafine_consumer_test_() ->
    {foreach, spawn, fun setup/0, fun cleanup/1, [
        fun set_topic_partitions_creates_relevant_node_fetchers/0,
        fun fetch_numeric_offset_is_passed_as_fetch_job/0,
        fun does_not_issue_fetch_job_until_all_topic_partitions_for_node_have_requests/0,
        fun paused_partitions_do_not_block_fetch_jobs/0,
        fun all_partitions_paused_does_not_create_job/0,
        fun fetch_earliest_offset_is_passed_as_list_offsets_job/0,
        fun fetch_latest_offset_is_passed_as_list_offsets_job/0,
        fun fetch_negative_offset_is_passed_as_list_offsets_job/0,
        fun fetch_multiple_numeric_offsets_are_batched/0,
        fun fetch_multiple_list_offsets_are_batched/0,
        fun does_not_request_offsets_until_all_topic_partitions_have_left_init/0,
        fun fetch_after_job_request_triggers_job/0,
        fun pausing_last_partition_after_job_request_triggers_job/0,
        fun list_offset_after_job_request_triggers_job/0,
        fun job_request_only_returns_fetches_for_the_requested_node/0,
        fun job_request_only_returns_list_offsets_for_the_requested_node/0,
        fun update_offsets_results_in_request_becoming_fetchable/0,
        fun update_offsets_to_non_numeric_results_in_list_offsets_job/0,
        fun update_offsets_updates_all_provided_offsets/0,
        fun uncompleted_fetches_can_be_requested_again/0,
        fun uncompleted_fetches_are_superseded_by_newer_fetches/0,
        fun repeat_requested_fetches_can_be_requested_again/0,
        fun repeat_requested_fetches_are_superseded_by_newer_fetches/0,
        fun fetch_request_arriving_before_completed_for_same_partition_is_in_next_job/0,
        fun list_offset_request_arriving_before_completed_for_same_partition_is_in_next_job/0,
        fun set_topic_partitions_updates_topic_partitions/0,
        fun set_topic_partitions_terminates_newest_pid_if_node_fetcher_restarts/0,
        fun set_topic_partitions_correctly_handles_pending_fetches/0,
        fun set_topic_partitions_correctly_handles_pending_list_offsets/0,
        fun set_topic_partitions_can_satisfy_pending_job_request_by_moving_fetch_node/0,
        fun set_topic_partitions_can_satisfy_pending_job_request_by_moving_offset_node/0,
        fun give_away_correctly_handles_pending_fetches/0,
        fun give_away_correctly_handles_pending_offsets/0,
        fun give_away_can_trigger_creation_of_new_and_termination_of_unused_node_fetchers/0,
        fun give_away_can_satisfy_pending_job_request_by_moving_fetch_node/0,
        fun give_away_can_satisfy_pending_job_request_by_moving_fetch_node_when_nodes_already_updated/0,
        fun give_away_can_satisfy_pending_job_request_by_moving_offset_node/0,
        fun give_away_can_satisfy_pending_job_request_by_moving_offset_node_when_nodes_already_updated/0,
        fun complete_job_with_complete_update_offset_and_give_away_works/0,
        fun partition_moving_node_will_be_in_next_job_request_after_give_away/0,
        fun partition_moving_node_will_not_be_in_next_job_request_if_completed/0,
        fun node_consumer_exit_makes_in_flight_requests_go_to_replacement/0
    ]}.

set_topic_partitions_creates_relevant_node_fetchers() ->
    TopicPartitionNodes = #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}},
    setup_topic_partitions(TopicPartitionNodes),
    {ok, Fetcher} = kafine_fetcher:start_link(?REF, ?FETCHER_METADATA),

    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(TopicPartitionNodes)),

    lists:foreach(
        fun(Broker) ->
            ?assert(meck:called(kafine_node_fetcher_sup, start_child, ['_', Fetcher, Broker, '_']))
        end,
        [#{node_id => 101}, #{node_id => 102}]
    ).

fetch_numeric_offset_is_passed_as_fetch_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(1, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}).

does_not_issue_fetch_job_until_all_topic_partitions_for_node_have_requests() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    timer:sleep(50),
    assert_no_job(),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),

    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }).

paused_partitions_do_not_block_fetch_jobs() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:pause(Fetcher, ?TOPIC_NAME_2, 0),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}
    }).

all_partitions_paused_does_not_create_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    kafine_fetcher:pause(Fetcher, ?TOPIC_NAME, 0),
    kafine_fetcher:pause(Fetcher, ?TOPIC_NAME_2, 0),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    timer:sleep(50),
    assert_no_job().

fetch_earliest_offset_is_passed_as_list_offsets_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, earliest, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(1, list_offsets, #{?TOPIC_NAME => #{0 => earliest}}).

fetch_latest_offset_is_passed_as_list_offsets_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, latest, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(1, list_offsets, #{?TOPIC_NAME => #{0 => latest}}).

fetch_negative_offset_is_passed_as_list_offsets_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, -1, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(1, list_offsets, #{?TOPIC_NAME => #{0 => -1}}).

fetch_multiple_numeric_offsets_are_batched() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }).

does_not_request_offsets_until_all_topic_partitions_have_left_init() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101, 2 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, earliest, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:pause(Fetcher, ?TOPIC_NAME_2, 2),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    timer:sleep(50),
    assert_no_job(),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, earliest, ?MODULE, ?CALLBACK_ARGS),
    assert_job(1, list_offsets, #{
        ?TOPIC_NAME => #{0 => earliest},
        ?TOPIC_NAME_2 => #{0 => earliest}
    }).

fetch_multiple_list_offsets_are_batched() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, earliest, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, -1, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(1, list_offsets, #{
        ?TOPIC_NAME => #{0 => earliest},
        ?TOPIC_NAME_2 => #{0 => -1}
    }).

fetch_after_job_request_triggers_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}}),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),

    assert_job(1, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}).

pausing_last_partition_after_job_request_triggers_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    timer:sleep(50),
    assert_no_job(),

    kafine_fetcher:pause(Fetcher, ?TOPIC_NAME, 2),

    assert_job(1, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}).

list_offset_after_job_request_triggers_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}}),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, latest, ?MODULE, ?CALLBACK_ARGS),

    assert_job(1, list_offsets, #{?TOPIC_NAME => #{0 => latest}}).

job_request_only_returns_fetches_for_the_requested_node() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 2, 456, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(1, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}),

    kafine_fetcher:request_job(Fetcher, 102, self()),

    assert_job(2, fetch, #{?TOPIC_NAME => #{2 => {456, ?MODULE, ?CALLBACK_ARGS}}}).

job_request_only_returns_list_offsets_for_the_requested_node() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, -1, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 2, -2, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(1, list_offsets, #{?TOPIC_NAME => #{0 => -1}}),

    kafine_fetcher:request_job(Fetcher, 102, self()),

    assert_job(2, list_offsets, #{?TOPIC_NAME => #{2 => -2}}).

update_offsets_results_in_request_becoming_fetchable() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, earliest, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, list_offsets, #{?TOPIC_NAME => #{0 => earliest}}),

    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => {update_offset, 123}}}
    ),
    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(2, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}).

update_offsets_to_non_numeric_results_in_list_offsets_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}),

    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => {update_offset, earliest}}}
    ),
    kafine_fetcher:request_job(Fetcher, 101, self()),

    assert_job(2, list_offsets, #{?TOPIC_NAME => #{0 => earliest}}).

update_offsets_updates_all_provided_offsets() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, earliest, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, latest, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, list_offsets, #{
        ?TOPIC_NAME => #{0 => earliest},
        ?TOPIC_NAME_2 => #{0 => latest}
    }),

    kafine_fetcher:complete_job(Fetcher, 1, 101, #{
        ?TOPIC_NAME => #{0 => {update_offset, 123}},
        ?TOPIC_NAME_2 => #{0 => {update_offset, 456}}
    }),

    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(2, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }).

uncompleted_fetches_can_be_requested_again() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % Queue a fetch and get the job for it
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }),

    % send a completion for only one of the requests
    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => completed}}
    ),

    % Queue another fetch
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 789, ?MODULE, ?CALLBACK_ARGS),

    % Request another job
    kafine_fetcher:request_job(Fetcher, 101, self()),
    % Both fetches should be returned
    assert_job(2, fetch, #{
        ?TOPIC_NAME => #{0 => {789, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }).

uncompleted_fetches_are_superseded_by_newer_fetches() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % Queue a fetch and get the job for it
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }),

    % send a new fetch request for both topic partitions
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 234, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 567, ?MODULE, ?CALLBACK_ARGS),

    % send a completion for only one of the original requests
    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => completed}}
    ),

    % Request another job
    kafine_fetcher:request_job(Fetcher, 101, self()),
    % Both fetches should be the newer fetches
    assert_job(2, fetch, #{
        ?TOPIC_NAME => #{0 => {234, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {567, ?MODULE, ?CALLBACK_ARGS}}
    }).

repeat_requested_fetches_can_be_requested_again() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % Queue a fetch and get the job for it
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }),

    % send a completion for only one of the requests
    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => completed}, ?TOPIC_NAME_2 => #{0 => repeat}}
    ),

    % Queue another fetch
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 789, ?MODULE, ?CALLBACK_ARGS),

    % Request another job
    kafine_fetcher:request_job(Fetcher, 101, self()),
    % Both fetches should be returned
    assert_job(2, fetch, #{
        ?TOPIC_NAME => #{0 => {789, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }).

repeat_requested_fetches_are_superseded_by_newer_fetches() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % Queue a fetch and get the job for it
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }),

    % send a new fetch request for both topic partitions
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 234, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 567, ?MODULE, ?CALLBACK_ARGS),

    % send a completion for only one of the original requests
    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => completed}, ?TOPIC_NAME_2 => #{0 => repeat}}
    ),

    % Request another job
    kafine_fetcher:request_job(Fetcher, 101, self()),
    % Both fetches should be the newer fetches
    assert_job(2, fetch, #{
        ?TOPIC_NAME => #{0 => {234, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {567, ?MODULE, ?CALLBACK_ARGS}}
    }).

fetch_request_arriving_before_completed_for_same_partition_is_in_next_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}}),

    % send a fetch request, have the node fetcher pick it up
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:request_job(Fetcher, 101, self()),

    % The node fetcher completes the request, invokes the callback, that sends another fetch request
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 124, ?MODULE, ?CALLBACK_ARGS),

    % Now our completion message arrives from the node fetcher
    kafine_fetcher:complete_job(Fetcher, 1, 101, #{?TOPIC_NAME => #{0 => completed}}),

    % Requesting another job should return the second fetch request
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(2, fetch, #{?TOPIC_NAME => #{0 => {124, ?MODULE, ?CALLBACK_ARGS}}}).

list_offset_request_arriving_before_completed_for_same_partition_is_in_next_job() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101}}),

    % send a fetch request, have the node fetcher pick it up
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:request_job(Fetcher, 101, self()),

    % The node fetcher completes the request, invokes the callback.
    % The callback decides it wants to skip to the latest offset
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, latest, ?MODULE, ?CALLBACK_ARGS),

    % Now our completion message arrives from the node fetcher
    kafine_fetcher:complete_job(Fetcher, 1, 101, #{?TOPIC_NAME => #{0 => completed}}),

    % Requesting another job should return the second fetch request
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(2, list_offsets, #{?TOPIC_NAME => #{0 => latest}}).

set_topic_partitions_updates_topic_partitions() ->
    % make some dummy pids for the brokers
    BrokerPids = #{101 => make_pid(), 102 => make_pid(), 103 => make_pid()},

    meck:expect(kafine_node_fetcher_sup, start_child, fun(_, _, #{node_id := NodeId}, _) ->
        {ok, maps:get(NodeId, BrokerPids)}
    end),

    TopicPartitionNodes = #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}},
    setup_topic_partitions(TopicPartitionNodes),
    {ok, Fetcher} = kafine_fetcher:start_link(?REF, ?FETCHER_METADATA),
    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(TopicPartitionNodes)),
    lists:foreach(
        fun(Broker = #{node_id := NodeId}) ->
            kafine_fetcher:set_node_fetcher(Fetcher, Broker, maps:get(NodeId, BrokerPids))
        end,
        ?BROKERS
    ),

    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            % moved
            0 => 103,
            % new
            1 => 101
            % 2 => 102 % removed
        },
        ?TOPIC_NAME_2 => #{
            % unmoved
            0 => 101,
            % new
            1 => 101
        }
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    meck:reset(kafine_node_fetcher_sup),

    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(NewTopicPartitionNodes)),

    % stop 102, start 103
    ?assert(
        meck:called(kafine_node_fetcher_sup, terminate_child, ['_', maps:get(102, BrokerPids)])
    ),
    ?assert(meck:called(kafine_node_fetcher_sup, start_child, ['_', Fetcher, #{node_id => 103}, '_'])),
    % 101 should have been left alone
    ?assertNot(meck:called(kafine_node_fetcher_sup, start_child, ['_', #{node_id => 101}, '_'])),
    ?assertNot(
        meck:called(kafine_node_fetcher_sup, terminate_child, ['_', maps:get(101, BrokerPids)])
    ).

set_topic_partitions_terminates_newest_pid_if_node_fetcher_restarts() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}}),

    meck:reset(kafine_node_fetcher_sup),

    % Node fetcher 102 restarts, new pid registers with fetcher
    NewBroker102Pid = make_pid(),
    kafine_fetcher:set_node_fetcher(Fetcher, #{node_id => 102}, NewBroker102Pid),

    NewTopicPartitionNodes = #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}},
    setup_topic_partitions(NewTopicPartitionNodes),

    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(NewTopicPartitionNodes)),

    % stop the 102 fetcher with the new pid
    ?assert(meck:called(kafine_node_fetcher_sup, terminate_child, ['_', NewBroker102Pid])).

set_topic_partitions_correctly_handles_pending_fetches() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % should move to node 103
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    % shouldn't be accepted - this partition isn't currently managed
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 1, 789, ?MODULE, ?CALLBACK_ARGS),
    % should remain on node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),

    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            % moved
            0 => 103,
            % new
            1 => 101
            % 2 => 102 % removed
        },
        ?TOPIC_NAME_2 => #{
            % unmoved
            0 => 101,
            % new
            1 => 101
        }
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(NewTopicPartitionNodes)),

    % should be valid now that the partition is managed
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 1, 101112, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),

    timer:sleep(50),
    assert_no_job(),

    % another request comes in for TOPIC_NAME/1, which should now be valid
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 1, 890, ?MODULE, ?CALLBACK_ARGS),

    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{
            1 => {890, ?MODULE, ?CALLBACK_ARGS}
        },
        ?TOPIC_NAME_2 => #{
            0 => {456, ?MODULE, ?CALLBACK_ARGS},
            1 => {101112, ?MODULE, ?CALLBACK_ARGS}
        }
    }),

    kafine_fetcher:request_job(Fetcher, 103, self()),
    assert_job(2, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}).

set_topic_partitions_correctly_handles_pending_list_offsets() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % should move to node 103
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, earliest, ?MODULE, ?CALLBACK_ARGS),
    % shouldn't be accepted - this partition isn't currently managed
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 1, -1, ?MODULE, ?CALLBACK_ARGS),
    % should remain on node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, latest, ?MODULE, ?CALLBACK_ARGS),

    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            % moved
            0 => 103,
            % new
            1 => 101
            % 2 => 102 % removed
        },
        ?TOPIC_NAME_2 => #{
            % unmoved
            0 => 101,
            % new
            1 => 101
        }
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(NewTopicPartitionNodes)),

    % should be valid now that the partition is managed
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 1, -2, ?MODULE, ?CALLBACK_ARGS),

    % but nothing should happen yet because the TOPIC_NAME/1 request came before we were using that partition
    timer:sleep(50),
    assert_no_job(),

    % another request comes in for TOPIC_NAME/1, which should now be valid
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 1, -4, ?MODULE, ?CALLBACK_ARGS),

    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, list_offsets, #{
        ?TOPIC_NAME => #{
            1 => -4
        },
        ?TOPIC_NAME_2 => #{
            0 => latest,
            1 => -2
        }
    }),

    kafine_fetcher:request_job(Fetcher, 103, self()),
    assert_job(2, list_offsets, #{?TOPIC_NAME => #{0 => earliest}}).

set_topic_partitions_can_satisfy_pending_job_request_by_moving_fetch_node() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % These currently need node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),

    % no fetches currently use this node
    kafine_fetcher:request_job(Fetcher, 102, self()),

    NewTopicPartitionNodes = #{
        % moved
        ?TOPIC_NAME => #{0 => 102},
        ?TOPIC_NAME_2 => #{0 => 101}
        % TOPIC_NAME/2 removed
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(NewTopicPartitionNodes)),

    % first fetch should've moved to node 102, triggering the job request
    assert_job(1, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}),

    % second fetch should still be on node 101
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(2, fetch, #{?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}}).

set_topic_partitions_can_satisfy_pending_job_request_by_moving_offset_node() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % These currently need node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, earliest, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, latest, ?MODULE, ?CALLBACK_ARGS),

    % no fetches currently use this node
    kafine_fetcher:request_job(Fetcher, 102, self()),

    NewTopicPartitionNodes = #{
        % moved
        ?TOPIC_NAME => #{0 => 102},
        ?TOPIC_NAME_2 => #{0 => 101}
        % TOPIC_NAME/2 removed
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(NewTopicPartitionNodes)),

    % first fetch should've moved to node 102, triggering the job request
    assert_job(1, list_offsets, #{?TOPIC_NAME => #{0 => earliest}}),

    % second fetch should still be on node 101
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(2, list_offsets, #{?TOPIC_NAME_2 => #{0 => latest}}).

give_away_correctly_handles_pending_fetches() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % should move to node 102
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    % should remain on node 102
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 2, 789, ?MODULE, ?CALLBACK_ARGS),
    % should remain on node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),

    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            % moved
            0 => 102,
            2 => 102
        },
        ?TOPIC_NAME_2 => #{0 => 101}
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }),

    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => give_away}}
    ),

    kafine_fetcher:request_job(Fetcher, 102, self()),
    assert_job(2, fetch, #{
        ?TOPIC_NAME => #{
            0 => {123, ?MODULE, ?CALLBACK_ARGS},
            2 => {789, ?MODULE, ?CALLBACK_ARGS}
        }
    }).

give_away_correctly_handles_pending_offsets() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % should move to node 102
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, earliest, ?MODULE, ?CALLBACK_ARGS),
    % should remain on node 102
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 2, -1, ?MODULE, ?CALLBACK_ARGS),
    % should remain on node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, latest, ?MODULE, ?CALLBACK_ARGS),

    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            % moved
            0 => 102,
            2 => 102
        },
        ?TOPIC_NAME_2 => #{0 => 101}
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, list_offsets, #{
        ?TOPIC_NAME => #{0 => earliest},
        ?TOPIC_NAME_2 => #{0 => latest}
    }),

    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => give_away}}
    ),

    kafine_fetcher:request_job(Fetcher, 102, self()),
    assert_job(2, list_offsets, #{
        ?TOPIC_NAME => #{
            0 => earliest,
            2 => -1
        }
    }).

give_away_can_trigger_creation_of_new_and_termination_of_unused_node_fetchers() ->
    % make some dummy pids for the brokers
    BrokerPids = #{101 => make_pid(), 102 => make_pid(), 103 => make_pid()},

    meck:expect(kafine_node_fetcher_sup, start_child, fun(_, _, #{node_id := NodeId}, _) ->
        {ok, maps:get(NodeId, BrokerPids)}
    end),

    TopicPartitionNodes = #{?TOPIC_NAME => #{0 => 101, 2 => 102}},
    setup_topic_partitions(TopicPartitionNodes),

    {ok, Fetcher} = kafine_fetcher:start_link(?REF, ?FETCHER_METADATA),
    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(TopicPartitionNodes)),
    lists:foreach(
        fun(Broker = #{node_id := NodeId}) ->
            kafine_fetcher:set_node_fetcher(Fetcher, Broker, maps:get(NodeId, BrokerPids))
        end,
        ?BROKERS
    ),

    % The fetch that triggers the give_away
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),

    % partition from 101 has moved to 103
    NewTopicPartitionNodes = #{?TOPIC_NAME => #{0 => 103, 2 => 102}},
    setup_topic_partitions(NewTopicPartitionNodes),

    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}),

    meck:reset(kafine_metadata_cache),

    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => give_away}}
    ),

    % stop 101, start 103
    meck:wait(
        kafine_node_fetcher_sup, terminate_child, ['_', maps:get(101, BrokerPids)], ?WAIT_TIMEOUT_MS
    ),
    meck:wait(
        kafine_node_fetcher_sup, start_child, ['_', Fetcher, #{node_id => 103}, '_'], ?WAIT_TIMEOUT_MS
    ),
    ?assertNotCalled(kafine_node_fetcher_sup, terminate_child, ['_', Fetcher, #{node_id => 102}]),
    % we should have refreshed metadata during this process
    ?assertCalled(kafine_metadata_cache, refresh, [?REF, [?TOPIC_NAME]]).

give_away_can_satisfy_pending_job_request_by_moving_fetch_node() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % These currently need node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),

    % no fetches currently use this node
    kafine_fetcher:request_job(Fetcher, 102, self()),

    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            % swapped nodes
            0 => 102,
            2 => 101
        },
        ?TOPIC_NAME_2 => #{0 => 101}
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }),

    % this request no longer lives on node 101
    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => give_away}, ?TOPIC_NAME_2 => #{0 => completed}}
    ),

    % node 102 job request should be satisfied
    assert_job(2, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}).

% This test differs from the above because here, kafine_fetcher has already updated its
% understanding of what partitions are handled by what nodes. So when the give_away comes in from
% the second node, no node mapping update is required, but the pending job request must still be
% fulfilled.
give_away_can_satisfy_pending_job_request_by_moving_fetch_node_when_nodes_already_updated() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}}),

    % This needs 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    % This needs 102
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 2, 456, ?MODULE, ?CALLBACK_ARGS),

    % request on both nodes
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}),

    kafine_fetcher:request_job(Fetcher, 102, self()),
    assert_job(2, fetch, #{?TOPIC_NAME => #{2 => {456, ?MODULE, ?CALLBACK_ARGS}}}),

    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            % swapped nodes
            0 => 102,
            2 => 101
        }
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    % 101 replies with give_away
    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => give_away}}
    ),

    % 101 requests a new job. This can't be fulfilled until 102 gives away its request
    kafine_fetcher:request_job(Fetcher, 101, self()),

    % 102 completes its job and gives away t/2
    kafine_fetcher:complete_job(
        Fetcher,
        2,
        102,
        #{?TOPIC_NAME => #{2 => give_away}}
    ),

    % The request from 101 should now be fulfilled with the given away request
    assert_job(3, fetch, #{?TOPIC_NAME => #{2 => {456, ?MODULE, ?CALLBACK_ARGS}}}).

give_away_can_satisfy_pending_job_request_by_moving_offset_node() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}, ?TOPIC_NAME_2 => #{0 => 101}}),

    % These currently need node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, earliest, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, latest, ?MODULE, ?CALLBACK_ARGS),

    % no fetches currently use this node
    kafine_fetcher:request_job(Fetcher, 102, self()),

    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            % swapped nodes
            0 => 102,
            2 => 101
        },
        ?TOPIC_NAME_2 => #{0 => 101}
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, list_offsets, #{
        ?TOPIC_NAME => #{0 => earliest},
        ?TOPIC_NAME_2 => #{0 => latest}
    }),

    % this request no longer lives on node 101
    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => give_away}}
    ),

    % node 102 job request should be satisfied
    assert_job(2, list_offsets, #{?TOPIC_NAME => #{0 => earliest}}).

% This test differs from the above because here, kafine_fetcher has already updated its
% understanding of what partitions are handled by what nodes. So when the give_away comes in from
% the second node, no node mapping update is required, but the pending job request must still be
% fulfilled.
give_away_can_satisfy_pending_job_request_by_moving_offset_node_when_nodes_already_updated() ->
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}}),

    % This needs 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, earliest, ?MODULE, ?CALLBACK_ARGS),
    % This needs 102
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 2, latest, ?MODULE, ?CALLBACK_ARGS),

    % request on both nodes
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, list_offsets, #{?TOPIC_NAME => #{0 => earliest}}),

    kafine_fetcher:request_job(Fetcher, 102, self()),
    assert_job(2, list_offsets, #{?TOPIC_NAME => #{2 => latest}}),

    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            % swapped nodes
            0 => 102,
            2 => 101
        }
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    % 101 replies with give_away
    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{?TOPIC_NAME => #{0 => give_away}}
    ),

    % 101 requests a new job. This can't be fulfilled until 102 gives away its request
    kafine_fetcher:request_job(Fetcher, 101, self()),

    % 102 completes its job and gives away t/2
    kafine_fetcher:complete_job(
        Fetcher,
        2,
        102,
        #{?TOPIC_NAME => #{2 => give_away}}
    ),

    % The request from 101 should now be fulfilled with the given away request
    assert_job(3, list_offsets, #{?TOPIC_NAME => #{2 => latest}}).

complete_job_with_complete_update_offset_and_give_away_works() ->
    % need three topic partitions on the same broker for this test
    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 101}, ?TOPIC_NAME_2 => #{0 => 101}}),

    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 2, 789, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:request_job(Fetcher, 101, self()),
    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{
            0 => {123, ?MODULE, ?CALLBACK_ARGS},
            2 => {789, ?MODULE, ?CALLBACK_ARGS}
        },
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }),

    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            0 => 101,
            2 => 102
        },
        ?TOPIC_NAME_2 => #{0 => 101}
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    kafine_fetcher:complete_job(
        Fetcher,
        1,
        101,
        #{
            ?TOPIC_NAME => #{
                0 => completed,
                2 => give_away
            },
            ?TOPIC_NAME_2 => #{0 => {update_offset, 101112}}
        }
    ),

    % node 101 should still have the offset reset job, with the new offset
    kafine_fetcher:request_job(Fetcher, 101, self()),
    % but we'll need to fetch from TOPIC_NAME/0 to see it
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 124, ?MODULE, ?CALLBACK_ARGS),
    assert_job(2, fetch, #{
        ?TOPIC_NAME  => #{0 => {124, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {101112, ?MODULE, ?CALLBACK_ARGS}}
    }),

    % node 102 should have the give_away job
    kafine_fetcher:request_job(Fetcher, 102, self()),
    assert_job(3, fetch, #{?TOPIC_NAME => #{2 => {789, ?MODULE, ?CALLBACK_ARGS}}}).

partition_moving_node_will_be_in_next_job_request_after_give_away() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, fetcher, wait_for_job]
    ]),

    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}}),

    % Request on node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    % Request on node 102
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 2, 789, ?MODULE, ?CALLBACK_ARGS),

    % request job for both nodes
    kafine_fetcher:request_job(Fetcher, 101, self()),
    kafine_fetcher:request_job(Fetcher, 102, self()),
    assert_job(1, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}),
    assert_job(2, fetch, #{?TOPIC_NAME => #{2 => {789, ?MODULE, ?CALLBACK_ARGS}}}),
    meck:reset(kafine_node_fetcher),

    % Both partitions have swapped nodes
    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            0 => 102,
            2 => 101
        }
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    % Node 102 notices first and sends give_away
    kafine_fetcher:complete_job(Fetcher, 2, 102, #{?TOPIC_NAME => #{2 => give_away}}),

    % Then requests its next job
    kafine_fetcher:request_job(Fetcher, 102, self()),

    % The fetcher should be unable to fulfil the request immediately
    ?assertReceived({[kafine, fetcher, wait_for_job], _, _, #{node_id := 102}}),

    % Shouldn't have received a response to the job
    assert_no_job(),

    % Now node 101 sends its give away
    kafine_fetcher:complete_job(Fetcher, 1, 102, #{?TOPIC_NAME => #{0 => give_away}}),

    % Node 101 should receive the fetch request originally sent to partition 2
    assert_job(3, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}).

partition_moving_node_will_not_be_in_next_job_request_if_completed() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, fetcher, wait_for_job]
    ]),

    Fetcher = make_fetcher(?REF, #{?TOPIC_NAME => #{0 => 101, 2 => 102}}),

    % Request on node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    % Request on node 102
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 2, 789, ?MODULE, ?CALLBACK_ARGS),

    % request job for both nodes
    kafine_fetcher:request_job(Fetcher, 101, self()),
    kafine_fetcher:request_job(Fetcher, 102, self()),
    assert_job(1, fetch, #{?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}}}),
    assert_job(2, fetch, #{?TOPIC_NAME => #{2 => {789, ?MODULE, ?CALLBACK_ARGS}}}),
    meck:reset(kafine_node_fetcher),

    % Both partitions have swapped nodes
    NewTopicPartitionNodes = #{
        ?TOPIC_NAME => #{
            0 => 102,
            2 => 101
        }
    },
    setup_topic_partitions(NewTopicPartitionNodes),

    % Node 102 notices first and sends give_away
    kafine_fetcher:complete_job(Fetcher, 2, 102, #{?TOPIC_NAME => #{2 => give_away}}),

    % Now node 101 actually managed to complete the request
    kafine_fetcher:complete_job(Fetcher, 1, 102, #{?TOPIC_NAME => #{0 => completed}}),

    % Node 102 requests a new job
    kafine_fetcher:request_job(Fetcher, 102, self()),

    % The fetcher should be unable to fulfil the request immediately
    ?assertReceived({[kafine, fetcher, wait_for_job], _, _, #{node_id := 102}}),
    % Shouldn't have received a response to the job
    assert_no_job().

node_consumer_exit_makes_in_flight_requests_go_to_replacement() ->
    telemetry_test:attach_event_handlers(self(), [
        [kafine, fetcher, job_aborted]
    ]),

    TopicPartitionNodes = #{?TOPIC_NAME => #{0 => 101}, ?TOPIC_NAME_2 => #{0 => 101}},
    Fetcher = make_fetcher(?REF, TopicPartitionNodes),

    % setup: return a dummy pid for node 101 that forwards messages to the test
    TestPid = self(),
    ForwardLoop = fun ForwardLoop() ->
        receive
            Msg -> TestPid ! Msg,
            ForwardLoop()
        end
    end,
    Pid1 = proc_lib:spawn_link(ForwardLoop),
    meck:expect(kafine_node_fetcher_sup, start_child,
        fun(_, _, #{node_id := 101}, _) -> {ok, Pid1};
        (_, _, _, _) -> {ok, self()}
        end),

    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(TopicPartitionNodes)),

    % register node 101
    kafine_fetcher:set_node_fetcher(Fetcher, #{node_id => 101}, Pid1),

    % make fetchs request that will go to node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME, 0, 123, ?MODULE, ?CALLBACK_ARGS),
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 455, ?MODULE, ?CALLBACK_ARGS),

    % request a job with that fetch
    kafine_fetcher:request_job(Fetcher, 101, Pid1),
    % and wait until we get the job
    assert_job(1, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {455, ?MODULE, ?CALLBACK_ARGS}}
    }),

    % make another fetch going to node 101
    kafine_fetcher:fetch(Fetcher, ?TOPIC_NAME_2, 0, 456, ?MODULE, ?CALLBACK_ARGS),

    % exit the current node fetcher
    unlink(Pid1),
    exit(Pid1, kill),
    ?assertReceived({[kafine, fetcher, job_aborted], _, _, #{job_id := 1}}),

    % A replacement node fetcher is started and registers
    Pid2 = proc_lib:spawn_link(ForwardLoop),
    kafine_fetcher:set_node_fetcher(Fetcher, #{node_id => 101}, Pid2),

    % request a job with the new node fetcher
    kafine_fetcher:request_job(Fetcher, 101, Pid2),
    % we expect the fetch which was at the old node fetcher, plus the new fetch
    assert_job(2, fetch, #{
        ?TOPIC_NAME => #{0 => {123, ?MODULE, ?CALLBACK_ARGS}},
        ?TOPIC_NAME_2 => #{0 => {456, ?MODULE, ?CALLBACK_ARGS}}
    }).

setup_topic_partitions(TopicPartitionNodes) ->
    DefaultTopicPartitionInfo = #{
        ?TOPIC_NAME => #{
            0 => #{leader => 101},
            1 => #{leader => 102},
            2 => #{leader => 101},
            3 => #{leader => 102}
        },
        ?TOPIC_NAME_2 => #{
            0 => #{leader => 101},
            1 => #{leader => 102},
            2 => #{leader => 101},
            3 => #{leader => 102}
        }
    },

    TopicPartitionInfoBase = maps:filter(
        fun(Topic, _Partitions) ->
            maps:is_key(Topic, TopicPartitionNodes)
        end,
        DefaultTopicPartitionInfo
    ),

    TopicPartitionInfo = kafine_topic_partition_data:map(
        fun(Topic, Partition, Info) ->
            case kafine_topic_partition_data:get(Topic, Partition, TopicPartitionNodes, undefined) of
                undefined -> Info;
                Leader -> Info#{leader => Leader}
            end
        end,
        TopicPartitionInfoBase
    ),

    meck:expect(kafine_metadata_cache, brokers, fun(_) -> ?BROKERS end),
    meck:expect(kafine_metadata_cache, partitions, fun(_R, _T) -> TopicPartitionInfo end).


make_fetcher(Ref, TopicPartitionNodes) ->
    setup_topic_partitions(TopicPartitionNodes),
    {ok, Fetcher} = kafine_fetcher:start_link(Ref, ?FETCHER_METADATA),

    ok = kafine_fetcher:set_topic_partitions(Fetcher, get_topic_partitions(TopicPartitionNodes)),

    meck:expect(kafine_node_fetcher_sup, start_child,
        fun(_, Owner, Broker, _) ->
            kafine_fetcher:set_node_fetcher(Ref, Broker, Owner),
            {ok, self()}
        end
    ),

    Fetcher.

get_topic_partitions(TopicPartitionNodes) ->
    maps:map(
        fun(_Topic, Partitions) ->
            maps:keys(Partitions)
        end,
        TopicPartitionNodes
    ).

% Make a disposable pid we can assert on
make_pid() ->
    proc_lib:spawn_link(fun() -> receive
        after infinity -> ok
        end end).

assert_job(JobId, JobType, Requests) ->
    ?assertWait(
        kafine_node_fetcher,
        job,
        ['_', meck:is(fun(R) -> R =:= {JobId, JobType, Requests} end)],
        ?WAIT_TIMEOUT_MS
    ).

assert_no_job() ->
    ?assertNotCalled(kafine_node_fetcher, job, ['_', '_']).
