-module(raws_ingest_server).
-behaviour(gen_server).
-define(SERVER, ?MODULE).
-include("raws_ingest.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/5]).
-export([report_errors/0,clear_errors/0,update_now/0,acquire_observations/3]).
-export([is_station_selector/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Token,StationInfos,VarIds,TimeoutMins,Method) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Token,StationInfos,VarIds,TimeoutMins,calendar:local_time(),Method,[]], []).


report_errors() ->
  gen_server:call(?SERVER,report_errors).

clear_errors() ->
  gen_server:call(?SERVER,clear_errors).

update_now() ->
  gen_server:call(?SERVER,update_now).

-spec acquire_observations(station_selector(),[atom()],{calendar:datetime(),calendar:datetime()}) -> ok|{error,any()}.
acquire_observations(SSel,VarIds,{From,To}) ->
  true = is_station_selector(SSel),
  gen_server:call(?SERVER,{acquire_observations,SSel,VarIds,From,To},30000).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    ?SERVER ! update_timeout,
    {ok, Args}.


handle_call(Request, _From, State=[Token,StationSel0,VarIds,TimeoutMins,UpdateFrom,Method,Errors]) ->
  case Request of
    {acquire_observations,StationSelR,VarIdsR,From,To} ->
      case resolve_station_selector(StationSelR,VarIdsR,Token) of
        {station_list, StationIds} ->
          case safe_retrieve_observations(Method,Token,From,To,StationIds,VarIdsR) of
            {[], StInfos, Obss} ->
              store_station_infos(StInfos),
              store_observations(Obss),
              {reply, ok, State};
            {NewErrors,_,_} ->
              {reply, {error, NewErrors}, State}
          end;
        _ ->
          {reply, {error, io_lib:format("Unable to resolve station selector ~p.",[StationSelR])}, State}
      end;
    update_now ->
      {NewErrors,UpdateFrom1,StationSel1} = update_observations_now(State),
      {reply, ok, [Token,StationSel1,VarIds,TimeoutMins,UpdateFrom1,Method,NewErrors ++ Errors]};
    report_errors ->
      {reply, Errors, State};
    clear_errors ->
      {reply, ok, [Token,StationSel0,VarIds,TimeoutMins,UpdateFrom,Method,[]]}
  end.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(update_timeout,State = [Token,_StationSel,VarIds,TimeoutMins,_UpdateFrom,Method,Errors]) ->
  {NewErrors,UpdateFrom1,StationSel1} = update_observations_now(State),
  timer:send_after(TimeoutMins * 60 * 1000, update_timeout),
  {noreply, [Token,StationSel1,VarIds,TimeoutMins,UpdateFrom1,Method,NewErrors ++ Errors]};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

update_observations_now([Token,StationSel0,VarIds,_TimeoutMins,UpdateFrom,Method,_Errors]) ->
  TimeNow = calendar:universal_time(),
  StationSel = resolve_station_selector(StationSel0, VarIds, Token),
  case StationSel of
    {station_list, StationIds} ->
      {NewErrors,StationInfos,Obs} = safe_retrieve_observations(Method,Token,UpdateFrom,TimeNow,StationIds,VarIds),
      store_station_infos(StationInfos),
      store_observations(Obs),
      case NewErrors of
        [] ->
          % only mark database updated till TimeNow if there were no errors
          {[],TimeNow,StationSel};
        _NotEmpty ->
          % since there were errors, do not consider the time interval UpdateFrom to TimeNow as processed
          {NewErrors,UpdateFrom,StationSel}
      end;
    _ ->
      {[{error,StationSel,TimeNow,unresolved_stations,"Could not resolve stations from region."}],UpdateFrom,StationSel}
  end.


-spec resolve_station_selector(station_selector(),[var_id()],string()) -> station_selector().
resolve_station_selector({station_list, Lst}, _, _) ->
  {station_list, Lst};
resolve_station_selector(Sel={region, {MinLat,MaxLat}, {MinLon,MaxLon}}, VarIds, Token) ->
  try
    StationInfos = mesowest_json_ingest:list_raws_in_bbox({MinLat,MaxLat},{MinLon,MaxLon},VarIds,Token),
    Ids = lists:map(fun (#raws_station{id=Id}) -> Id end, StationInfos),
    {station_list, Ids}
  catch Bdy:Exc ->
    error_logger:error_msg("Failed to resolve station selector ~p due to exception~n~p (bdy ~p)~nstacktrace:~p~n", [Sel,Bdy,Exc,erlang:get_stacktrace()]),
    Sel
  end.

-spec is_station_selector(any()) -> boolean().
is_station_selector({station_list,Lst}) when is_list(Lst) ->
  true;
is_station_selector({region,{MinLat,MaxLat},{MinLon,MaxLon}}) when is_number(MinLat) and is_number(MaxLat)
                                                               and is_number(MinLon) and is_number(MaxLon) ->
  true;
is_station_selector(_) ->
  false.


store_station_infos(StInfos) ->
  mnesia:transaction(fun () -> lists:map(fun mnesia:write/1, StInfos) end).

store_observations(Obs) ->
  mnesia:transaction(fun () -> lists:map(fun mnesia:write/1, Obs) end).

safe_retrieve_observations(mesowest_json,Token,From,To,Sts,VarIds) ->
  try
    {StInfos,Obss} = mesowest_json_ingest:retrieve_observations(From,To,Sts,VarIds,Token),
    {[],StInfos,Obss}
  catch Cls:Exc ->
    error_logger:error_msg("raws_ingest_server encountered exception ~p:~p in retrieve_observations for stations ~p from ~w to ~w for vars ~w~nstacktrace: ~p",
                           [Cls,Exc,Sts,From,To,VarIds,erlang:get_stacktrace()]),
    {[{error,Sts,calendar:local_time(),Cls,Exc}], [], []}
  end;
safe_retrieve_observations(mesowest_web,_Token,_From,To,Sts,VarIds) ->
  Report = lists:map(fun (S) ->
        try
          {StInfo,Obs} = mesowest_ingest:retrieve_observations(S,To,VarIds),
          {ok,StInfo,Obs}
        catch Cls:Exc ->
          error_logger:error_msg("raws_ingest_server encountered exception ~p:~p in retrieve_observations for stations ~w at GMT time ~w for vars ~w~nstacktrace: ~p",
            [Cls,Exc,S,To,VarIds,erlang:get_stacktrace()]),
          {error,S,calendar:local_time(),Cls,Exc}
        end end, Sts),
  {Oks,Errors} = lists:partition(fun ({ok,_,_}) -> true; (_) -> false end, Report),
  {_,StInfos,Obss} = lists:unzip3(Oks),
  {Errors,StInfos,lists:flatten(Obss)}.


