-module(kafine_response_telemetry).

-export([
    enrich_span/2
]).

enrich_span(Response, {Measurements, Metadata}) ->
    % not_available is temporary until we figure out how to best expose deeper error codes
    ErrorCode = maps:get(error_code, Response, not_available),
    {Measurements, Metadata#{error_code => ErrorCode}}.
