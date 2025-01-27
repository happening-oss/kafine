-module(kafine_request_telemetry).

-export([
    request_labels/2,
    request_labels/3,
    request_labels/4
]).

request_labels(Request, ApiVersion) ->
    #{api_key => Request, api_version => ApiVersion}.

request_labels(Request, ApiVersion, GroupId) ->
    #{api_key => Request, api_version => ApiVersion, group_id => GroupId}.

request_labels(Request, ApiVersion, Topic, Partition) ->
    #{api_key => Request, api_version => ApiVersion, topic => Topic, partition => Partition}.
