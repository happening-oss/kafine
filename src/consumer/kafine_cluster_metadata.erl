-module(kafine_cluster_metadata).
-export([from/1]).
-export([partitions/2]).
-export_type([t/0]).

% Abstraction over kafine_metadata_cache (which is internal). Passed to assignor callback. At the moment, it's just a
% one-for-one wrapper over kafine_metadata_cache, but that might change, so...

-opaque t() :: kafine_metadata_cache:ref().

% Constructor
from(Ref) ->
    Ref.

partitions(Ref, Topics) ->
    kafine_metadata_cache:partitions(Ref, Topics).
