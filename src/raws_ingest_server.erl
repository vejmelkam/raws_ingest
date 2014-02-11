-module(raws_ingest_server).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/3]).
-export([report_errors/0,clear_errors/0,update_now/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(StationIds,VarNames,TimeoutMins) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [StationIds,VarNames,TimeoutMins,[]], []).


report_errors() ->
  gen_server:call(?SERVER,report_errors).

clear_errors() ->
  gen_server:call(?SERVER,clear_errors).

update_now() ->
  gen_server:call(?SERVER,update_now).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    ?SERVER ! update_timeout,
    {ok, Args}.


handle_call(Request, _From, State=[StationIds,VarNames,TimeoutMins,Errors]) ->
  case Request of
    update_now ->
      {NewErrors,StationInfos,Obs} = safe_retrieve_observations(StationIds,calendar:universal_time(),VarNames),
      store_station_infos(StationInfos),
      store_observations(Obs),
      {reply, ok, [StationIds,VarNames,TimeoutMins,NewErrors ++ Errors]};
    report_errors ->
      {reply, Errors, State};
    clear_errors ->
      {reply, ok, [StationIds,VarNames,TimeoutMins,[]]}
  end.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(update_timeout,[StationIds,VarNames,TimeoutMins,Errors]) ->
  {NewErrors,StationInfos,Obs} = safe_retrieve_observations(StationIds,calendar:universal_time(),VarNames),
  store_station_infos(StationInfos),
  store_observations(Obs),
  timer:send_after(TimeoutMins * 60 * 1000, update_timeout),
  {noreply, [StationIds,VarNames,TimeoutMins,NewErrors ++ Errors]};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

store_station_infos(StInfos) ->
  mnesia:transaction(fun () -> lists:map(fun mnesia:write/1, StInfos) end).

store_observations(Obs) ->
  mnesia:transaction(fun () -> lists:map(fun mnesia:write/1, Obs) end).

safe_retrieve_observations(Sts,TimeGMT,Vars) ->
  Report = lists:map(fun (S) ->
        try
          {StInfo,Obs} = mesowest_ingest_csv:retrieve_observations(S,TimeGMT,Vars),
          {ok,StInfo,Obs}
        catch Cls:Exc ->
          {error,S,calendar:local_time(),Cls,Exc}
        end end, Sts),
  {Oks,Errors} = lists:partition(fun ({ok,_,_}) -> true; (_) -> false end, Report),
  {_,StInfos,Obss} = lists:unzip3(Oks),
  {Errors,StInfos,lists:flatten(Obss)}.

