-module(raws_ingest_server).
-behaviour(gen_server).
-define(SERVER, ?MODULE).
-include("raws_ingest.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/5]).
-export([report_errors/0,clear_errors/0,update_now/1]).
-export([acquire_observations/4,acquire_stations/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Cfg,SSel,VarIds,TimeoutMins,Method) ->
  Method = mesowest_json,
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Cfg,SSel,VarIds,TimeoutMins,calendar:local_time(),Method,[]], []).


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


-spec acquire_stations(station_selector(),[atom()]) -> [#raws_station{}]|{error,any()}.
acquire_stations(SSel,WithVars) ->
  true = raws_ingest:is_station_selector(SSel),
  gen_server:call(?SERVER,{acquire_stations,SSel,WithVars},120000).



%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
  ?SERVER ! update_timeout,
  {ok, Args}.


handle_call({acquire_stations,SSel,WithVars}, _From, State=[Cfg,_,_,_,_,Method,_]) ->
  Token = proplists:get_value(token, Cfg),
  {reply, safe_acquire_stations(Method,SSel,WithVars,Token), State};
handle_call({acquire_observations,SSel,VarIdsR,From,To}, _From, State=[Cfg,_,_,_,_,Method,_]) ->
  Token = proplists:get_value(token, Cfg),
  case safe_acquire_observations(Method,Token,From,To,SSel,VarIdsR) of
    {[], StInfos, Obss} ->
      store_stations(StInfos),
      store_observations(Obss),
      {reply, Obss, State};
    {NewErrors,_,_} ->
      % errors not incurred during updates are not kept in the state but reported immeditely.
      {reply, {error, NewErrors}, State};
    _ ->
      {reply, {error, io_lib:format("Unable to resolve station selector ~p.",[SSel])}, State}
  end;
handle_call(update_now, _From, State=[Cfg,StationSel0,VarIds,TimeoutMins,_UpdateFrom,Method,Errors]) ->
  case StationSel0 of
    empty_station_selector ->
      {reply, ok, State};
    _ ->
      {NewErrors,UpdateFrom1,_} = update_observations_now(State),
      {reply, ok, [Cfg,StationSel0,VarIds,TimeoutMins,UpdateFrom1,Method,NewErrors ++ Errors]}
  end;
handle_call(report_errors, _From, State=[_,_,_,_,_,_,Errors]) ->
  {reply, Errors, State};
handle_call(clear_errors, _From, [Cfg,StationSel0,VarIds,TimeoutMins,UpdateFrom,Method,_]) ->
  {reply, ok, [Cfg,StationSel0,VarIds,TimeoutMins,UpdateFrom,Method,[]]}.


handle_cast(_Msg, State) ->
  {noreply, State}.


handle_info(update_timeout,State = [Cfg,StationSel,VarIds,TimeoutMins,_UpdateFrom,Method,Errors]) ->
  {NewErrors,UpdateFrom1,_} = update_observations_now(State),
  timer:send_after(TimeoutMins * 60 * 1000, update_timeout),
  {noreply, [Cfg,StationSel,VarIds,TimeoutMins,UpdateFrom1,Method,NewErrors ++ Errors]};
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
update_observations_now([_,empty_station_selector,_,_,_,_,_]) ->
  {[],calendar:universal_time(),empty_station_selector};
update_observations_now([Cfg,StationSel,VarIds,_TimeoutMins,UpdateFrom,Method,_Errors]) ->
  Token = proplists:get_value(token, Cfg),
  TimeNow = calendar:universal_time(),
  error_logger:info_msg("raws_ingest_server: performing update of observations at ~p", [TimeNow]),
  {NewErrors,StationInfos,Obs} = safe_acquire_observations(Method,Token,UpdateFrom,TimeNow,StationSel,VarIds),
  store_stations(StationInfos),
  store_observations(Obs),
  case NewErrors of
    [] ->
      % only mark database updated till TimeNow if there were no errors
      {[],TimeNow,StationSel};
    _NotEmpty ->
      % since there were errors, do not consider the time interval UpdateFrom to TimeNow as processed
      {NewErrors,UpdateFrom,StationSel}
  end.


%% @doc Acquire stations inside given bounding box, reporting given variables. Use token Token.
-spec safe_acquire_stations(atom(),station_selector(),[var_id()],string()) -> [#raws_station{}].
safe_acquire_stations(mesowest_json,{region, LatRng={MinLat,MaxLat},LonRng={MinLon,MaxLon}},WithVars,Token) ->
  try
    error_logger:info_msg("raws_ingest_server: acquiring stations in bbox ~p x ~p with vars ~p", [LatRng,LonRng,WithVars]),
    S = mesowest_json_ingest:acquire_stations_in_bbox(LatRng,LonRng,WithVars,Token),
    store_stations(S),
    error_logger:info_msg("raws_ingest_server: acquired ~p stations in bbox ~p-~p lat, ~p-~p lon.",
      [length(S),MinLat,MaxLat,MinLon,MaxLon]),
    S
  catch Bdy:Exc ->
    error_logger:error_msg("Failed to find stations in bbox [~p,~p], [~p,~p]~nstacktrace:~p.", [MinLat,MaxLat,MinLon,MaxLon,erlang:get_stacktrace()]),
    {error, Bdy, Exc}
end;
safe_acquire_stations(mesowest_json,{station_list, Ids}, WithVars, Token) ->
  try
    S = mesowest_json_ingest:acquire_listed_stations(Ids,WithVars,Token),
    store_stations(S),
    error_logger:info_msg("raws_ingest_server: acquired ~p stations from list with ~p entries.", [length(S), length(Ids)]),
    S
  catch Bdy:Exc ->
    error_logger:error_msg("raws_ingest_server: failed to acquire stations from list ~p~nstacktrace:~p.", [Ids,erlang:get_stacktrace()]),
    {error, Bdy, Exc}
end;
safe_acquire_stations(_,empty_station_selector, _, _) ->
  [].


%% @doc Store station information for a list of stations.
-spec store_stations([#raws_station{}]) -> [{insert,0,1}].
store_stations(StInfos) ->
  lists:map(fun raws_sql:store_raws_station/1, StInfos).


%% @doc Store a list of observations in the raws_observation table.
store_observations(Obs) ->
  lists:map(fun raws_sql:store_raws_obs/1, Obs).


%% @doc Acquire observations from stations satisfying the station selector.
-spec safe_acquire_observations(atom(),string(),calendar:datetime(),calendar:datetime(),station_selector(),[atom()]) -> {[#raws_station{}],[#raws_obs{}]}.
safe_acquire_observations(mesowest_json,_,_,_,empty_station_selector,_) ->
  {[],[],[]};
safe_acquire_observations(mesowest_json,Token,From,To,SSel={station_list, Sts},VarIds) ->
  StIds = raws_ingest:stations_to_ids(Sts),
  try
    {StInfos,Obss} = mesowest_json_ingest:acquire_observations(From,To,StIds,VarIds,Token),
    error_logger:info_msg("raws_ingest_server: acquired ~p observations from ~p stations in interval [~p <-> ~p] via MesoWest/JSON.~n",
      [length(Obss),length(StInfos),to_esmf(From),to_esmf(To)]),
    {[],StInfos,Obss}
  catch Cls:Exc ->
    error_logger:error_msg("raws_ingest_server encountered exception ~p:~p in retrieve_observations for stations~n~p from ~w to ~w for vars ~w~nstacktrace: ~p.~n",
                           [Cls,Exc,SSel,From,To,VarIds,erlang:get_stacktrace()]),
    {[{error,SSel,calendar:local_time(),Cls,Exc}], [], []}
  end;
safe_acquire_observations(mesowest_json,Token,From,To,SSel,VarIds) ->
  Sts = safe_acquire_stations(mesowest_json,SSel,VarIds,Token),
  safe_acquire_observations(mesowest_json,Token,From,To,{station_list,Sts},VarIds).


%% @doc Convert an erlang datetime to the ESMF representation.
-spec to_esmf(calendar:datetime()) -> string().
to_esmf({{Y,M,D},{H,Min,S}}) ->
  lists:flatten(io_lib:format("~4..0B~2..0B~2..0B_~2..0B~2..0B~2..0B", [Y,M,D,H,Min,S])).

