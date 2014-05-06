-module(raws_ingest_server).
-behaviour(gen_server).
-define(SERVER, ?MODULE).
-include("raws_ingest.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/5]).
-export([report_errors/0,clear_errors/0,update_now/1,acquire_observations/4]).
-export([is_station_selector/1,acquire_stations_in_region/3]).

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

update_now(TimeoutS) ->
  gen_server:call(?SERVER,update_now,TimeoutS * 1000).

-spec acquire_observations(station_selector(),[atom()],{calendar:datetime(),calendar:datetime()},pos_integer()) -> [#raws_obs{}]|{error,any()}.
acquire_observations(SSel,VarIds,{From,To},TimeoutS) ->
  true = is_station_selector(SSel),
  gen_server:call(?SERVER,{acquire_observations,SSel,VarIds,From,To},TimeoutS*1000).


acquire_stations_in_region(LatRng={MinLat,MaxLat},LonRng={MinLon,MaxLon},WithVars)
    when is_number(MinLat) and is_number(MaxLat)
    and  is_number(MinLon) and is_number(MaxLon)
    and  is_list(WithVars) ->
  gen_server:call(?SERVER,{acquire_stations_in_bbox,LatRng,LonRng,WithVars},120000).



%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    ?SERVER ! update_timeout,
    {ok, Args}.


handle_call(Request, _From, State=[Token,StationSel0,VarIds,TimeoutMins,UpdateFrom,Method,Errors]) ->
  case Request of
    {acquire_stations_in_bbox,LatRng,LonRng,WithVars} ->
      {reply, safe_acquire_stations_in_bbox(LatRng,LonRng,WithVars,Token), State};
    {acquire_observations,StationSelR,VarIdsR,From,To} ->
      case raws_ingest:resolve_station_selector(StationSelR) of
        {station_list, StationIds} ->
          case safe_acquire_observations(Method,Token,From,To,StationIds,VarIdsR) of
            {[], StInfos, Obss} ->
              store_station_infos(StInfos),
              store_observations(Obss),
              {reply, Obss, State};
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
  StationSel = raws_ingest:resolve_station_selector(StationSel0),
  case StationSel of
    {station_list, StationIds} ->
      {NewErrors,StationInfos,Obs} = safe_acquire_observations(Method,Token,UpdateFrom,TimeNow,StationIds,VarIds),
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

-spec safe_acquire_stations_in_bbox({number(),number()},{number(),number()},[var_id()],string()) -> [#raws_station{}].
safe_acquire_stations_in_bbox(LatRng={MinLat,MaxLat},LonRng={MinLon,MaxLon},WithVars,Token) ->
  try
    S = mesowest_json_ingest:find_stations_in_bbox(LatRng,LonRng,WithVars,Token),
    store_station_infos(S),
    error_logger:info_msg("raws_ingest_server: acquired ~p stations in bbox ~p-~p lat, ~p-~p lon.~n",
      [length(S),MinLat,MaxLat,MinLon,MaxLon]),
    S
  catch Bdy:Exc ->
    error_logger:error_msg("Failed to find stations in bbox [~p,~p], [~p,~p]~nstacktrace:~p.~n", [MinLat,MaxLat,MinLon,MaxLon,erlang:get_stacktrace()]),
    {error, Bdy, Exc}
end.


store_station_infos(StInfos) ->
  mnesia:transaction(fun () -> lists:map(fun mnesia:write/1, StInfos) end).

store_observations(Obs) ->
  mnesia:transaction(fun () -> lists:map(fun mnesia:write/1, Obs) end).

safe_acquire_observations(mesowest_json,Token,From,To,Sts,VarIds) ->
  try
    {StInfos,Obss} = mesowest_json_ingest:retrieve_observations(From,To,Sts,VarIds,Token),
    error_logger:info_msg("raws_ingest_server: acquired ~p observations from ~p stations in interval [~p <-> ~p] via MesoWest/JSON.~n",
      [length(Obss),length(StInfos),to_esmf(From),to_esmf(To)]),
    {[],StInfos,Obss}
  catch Cls:Exc ->
    error_logger:error_msg("raws_ingest_server encountered exception ~p:~p in retrieve_observations for stations~n~p from ~w to ~w for vars ~w~nstacktrace: ~p.~n",
                           [Cls,Exc,Sts,From,To,VarIds,erlang:get_stacktrace()]),
    {[{error,Sts,calendar:local_time(),Cls,Exc}], [], []}
  end;
safe_acquire_observations(mesowest_web,_Token,_From,To,Sts,VarIds) ->
  Report = lists:map(fun (S) ->
        try
          {StInfo,Obs} = mesowest_ingest:retrieve_observations(S,To,VarIds),
          error_logger:info_msg("raws_ingest_server: acquired ~p observations from ~p stations via MesoWest/Web.~n",
            [length(Obs),length(StInfo)]),
          {ok,StInfo,Obs}
        catch Cls:Exc ->
          error_logger:error_msg("raws_ingest_server encountered exception ~p:~p in retrieve_observations for stations ~w at GMT time ~w for vars ~w~nstacktrace: ~p~n",
            [Cls,Exc,S,To,VarIds,erlang:get_stacktrace()]),
          {error,S,calendar:local_time(),Cls,Exc}
        end end, Sts),
  {Oks,Errors} = lists:partition(fun ({ok,_,_}) -> true; (_) -> false end, Report),
  {_,StInfos,Obss} = lists:unzip3(Oks),
  {Errors,StInfos,lists:flatten(Obss)}.


-spec is_station_selector(any()) -> boolean().
is_station_selector({station_list,Lst}) when is_list(Lst) ->
  true;
is_station_selector({region,{MinLat,MaxLat},{MinLon,MaxLon}}) when is_number(MinLat) and is_number(MaxLat)
                                                               and is_number(MinLon) and is_number(MaxLon) ->
  true;
is_station_selector({station_file,Path}) when is_list(Path) ->
  true;
is_station_selector(_) ->
  false.


to_esmf({{Y,M,D},{H,Min,S}}) ->
  lists:flatten(io_lib:format("~4..0B~2..0B~2..0B_~2..0B~2..0B~2..0B", [Y,M,D,H,Min,S])).
