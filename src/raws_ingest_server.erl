-module(raws_ingest_server).
-behaviour(gen_server).
-define(SERVER, ?MODULE).
-include("raws_ingest.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/5]).
-export([report_errors/0,clear_errors/0,update_now/1,acquire_observations/4]).
-export([acquire_stations_in_bbox/3]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Cfg,StationInfos,VarIds,TimeoutMins,Method) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Cfg,StationInfos,VarIds,TimeoutMins,calendar:local_time(),Method,[]], []).


report_errors() ->
  gen_server:call(?SERVER,report_errors).

clear_errors() ->
  gen_server:call(?SERVER,clear_errors).

update_now(TimeoutS) ->
  gen_server:call(?SERVER,update_now,TimeoutS * 1000).

-spec acquire_observations(station_selector(),[atom()],{calendar:datetime(),calendar:datetime()},pos_integer()) -> [#raws_obs{}]|{error,any()}.
acquire_observations(SSel,VarIds,{From,To},TimeoutS) ->
  true = raws_ingest:is_station_selector(SSel),
  gen_server:call(?SERVER,{acquire_observations,SSel,VarIds,From,To},TimeoutS*1000).


acquire_stations_in_bbox(LatRng={MinLat,MaxLat},LonRng={MinLon,MaxLon},WithVars)
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


handle_call({acquire_stations_in_bbox,LatRng,LonRng,WithVars}, _From, State=[Cfg|_NotUsed]) ->
  Token = proplists:get_value(token, Cfg),
  {reply, safe_acquire_stations_in_bbox(LatRng,LonRng,WithVars,Token), State};
handle_call({acquire_observations,StationSelR,VarIdsR,From,To}, _From, State=[Cfg,_,_,_,_,Method,_]) ->
  Token = proplists:get_value(token, Cfg),
  case raws_ingest:resolve_station_selector(StationSelR) of
    {station_list, StationIds} ->
      case safe_acquire_observations(Method,Token,From,To,StationIds,VarIdsR) of
        {[], StInfos, Obss} ->
          store_stations(StInfos),
          store_observations(Obss),
          {reply, Obss, State};
        {NewErrors,_,_} ->
          % errors not incurred during updates are not kept in the state but reported immeditely.
          {reply, {error, NewErrors}, State}
      end;
    _ ->
      {reply, {error, io_lib:format("Unable to resolve station selector ~p.",[StationSelR])}, State}
  end;
handle_call(update_now, _From, State=[Cfg,_StationSel0,VarIds,TimeoutMins,_UpdateFrom,Method,Errors]) ->
  {NewErrors,UpdateFrom1,StationSel1} = update_observations_now(State),
  {reply, ok, [Cfg,StationSel1,VarIds,TimeoutMins,UpdateFrom1,Method,NewErrors ++ Errors]};
handle_call(report_errors, _From, State=[_,_,_,_,_,_,Errors]) ->
  {reply, Errors, State};
handle_call(clear_errors, _From, [Cfg,StationSel0,VarIds,TimeoutMins,UpdateFrom,Method,_]) ->
  {reply, ok, [Cfg,StationSel0,VarIds,TimeoutMins,UpdateFrom,Method,[]]}.


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

%% @doc Update the observations using the stored station selector now.
update_observations_now([Cfg,StationSel0,VarIds,_TimeoutMins,UpdateFrom,Method,_Errors]) ->
  Token = proplists:get_value(token, Cfg),
  TimeNow = calendar:universal_time(),
  StationSel = raws_ingest:resolve_station_selector(StationSel0),
  case StationSel of
    {station_list, StationIds} ->
      {NewErrors,StationInfos,Obs} = safe_acquire_observations(Method,Token,UpdateFrom,TimeNow,StationIds,VarIds),
      store_stations(StationInfos),
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


%% @doc Acquire stations inside given bounding box, reporting given variables. Use token Token.
-spec safe_acquire_stations_in_bbox({number(),number()},{number(),number()},[var_id()],string()) -> [#raws_station{}].
safe_acquire_stations_in_bbox(LatRng={MinLat,MaxLat},LonRng={MinLon,MaxLon},WithVars,Token) ->
  try
    S = mesowest_json_ingest:find_stations_in_bbox(LatRng,LonRng,WithVars,Token),
    store_stations(S),
    error_logger:info_msg("raws_ingest_server: acquired ~p stations in bbox ~p-~p lat, ~p-~p lon.~n",
      [length(S),MinLat,MaxLat,MinLon,MaxLon]),
    S
  catch Bdy:Exc ->
    error_logger:error_msg("Failed to find stations in bbox [~p,~p], [~p,~p]~nstacktrace:~p.~n", [MinLat,MaxLat,MinLon,MaxLon,erlang:get_stacktrace()]),
    {error, Bdy, Exc}
end.


%% @doc Store station information for a list of stations.
-spec store_stations([#raws_station{}]) -> [{insert,0,1}].
store_stations(StInfos) ->
  lists:map(fun raws_sql:store_raws_station/1, StInfos).


%% @doc Store a list of observations in the raws_observation table.
store_observations(Obs) ->
  lists:map(fun raws_sql:store_raws_obs/1, Obs).


%% @doc Acquire observations from
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
  end.


%% @doc Convert an erlang datetime to the ESMF representation.
-spec to_esmf(calendar:datetime()) -> string().
to_esmf({{Y,M,D},{H,Min,S}}) ->
  lists:flatten(io_lib:format("~4..0B~2..0B~2..0B_~2..0B~2..0B~2..0B", [Y,M,D,H,Min,S])).

