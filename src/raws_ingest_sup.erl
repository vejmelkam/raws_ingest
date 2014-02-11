
-module(raws_ingest_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(Args) ->
    {ok, { {one_for_one, 5, 10}, [
          {raws_ingest_server,{raws_ingest_server,start_link,Args},
            permanent,brutal_kill,worker,[raws_ingest_server]}]}}.

