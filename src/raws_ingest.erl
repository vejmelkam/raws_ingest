-module(raws_ingest).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([start/0]).
-export([report_errors/0,clear_errors/0,update_now/0]).
-export([retrieve_station_info/1,retrieve_station_infos/1,resolve_station_selector/1]).
-export([retrieve_stations_in_region/2,acquire_stations_in_region/3]).
-export([retrieve_observations/3,acquire_observations/4]).
-include("raws_ingest.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(MAX_ACQ_INTERVAL,86400).


start() ->
  inets:start(),
  mnesia:create_schema([node()]),
  mnesia:start(),
  application:start(raws_ingest).


-spec report_errors() -> [{error,string(),calendar:datetime(),atom(),any()}].
report_errors() ->
  raws_ingest_server:report_errors().


-spec clear_errors() -> ok.
clear_errors() ->
  raws_ingest_server:clear_errors().


-spec update_now() -> ok.
update_now() ->
  raws_ingest_server:update_now().


-spec resolve_station_selector(station_selector()) -> station_selector().
resolve_station_selector({station_list, Lst}) ->
  {station_list, Lst};
resolve_station_selector({station_file,Path}) ->
  {ok,B} = file:read_file(Path),
  {station_list, string:tokens(binary_to_list(B),"\n")};
resolve_station_selector(Sel={region, {MinLat,MaxLat}, {MinLon,MaxLon}}) ->
  try
    StationInfos = raws_ingest:retrieve_stations_in_region({MinLat,MaxLat},{MinLon,MaxLon}),
    Ids = lists:map(fun (#raws_station{id=Id}) -> Id end, StationInfos),
    {station_list, Ids}
  catch Bdy:Exc ->
    error_logger:error_msg("Failed to resolve station selector ~p due to exception~n~p (bdy ~p)~nstacktrace:~p~n",
                           [Sel,Bdy,Exc,erlang:get_stacktrace()]),
    Sel
  end.


-spec acquire_observations(station_selector(),[var_id()],{calendar:datetime(),calendar:datetime()},pos_integer()) -> [#raws_obs{}]|{error,any()}.
acquire_observations(_SSel,_VarIds,{From,To},TimeoutS,Res) when From > To and is_list(_VarIds) and is_number(TimeoutS) ->
  Res;
acquire_observations(SSel,VarIds,{From,To},TimeoutS,Res) when is_list(VarIds) and is_number(TimeoutS) ->
  STo = calendar:datetime_to_gregorian_seconds(To),
  SFrom = calendar:datetime_to_gregorian_seconds(From),
  case STo - SFrom > ?MAX_ACQ_INTERVAL of
    true ->
      T = calendar:gregorian_seconds_to_datetime(SFrom + ?MAX_ACQ_INTERVAL),
      case raws_ingest_server:acquire_observations(SSel,VarIds,{From,T},TimeoutS) of
        {error, Reason} ->
          {error, Reason};
        Os ->
          acquire_observations(SSel,VarIds,{T,To},TimeoutS,[Os|Res])
      end;
    false ->
      raws_ingest_server:acquire_observations(SSel,VarIds,{From,To},TimeoutS)
  end.

acquire_observations(SSel,VarIds,{From,To},TimeoutS) when is_list(VarIds) and is_number(TimeoutS) ->
  acquire_observations(SSel,VarIds,{From,To},TimeoutS,[]).


-spec retrieve_observations(station_selector(),var_selector(),{calendar:datetime(),calendar:datetime()}) -> [#raws_obs{}].
retrieve_observations({station_list, Ss},VarSel,{From,To}) ->
  VarCheck = get_var_selector_fun(VarSel),
  case mnesia:transaction(
    fun() ->
        Q = qlc:q([X || X=#raws_obs{timestamp=T,station_id=S,var_id=V} <- mnesia:table(raws_obs),
                  T >= From, T =< To, lists:member(S,Ss), VarCheck(V)]),
        qlc:e(Q) end) of
    {atomic, R} ->
      R;
    Error ->
      Error
  end;
retrieve_observations({region, LatRng,LonRng},VarSel,{From,To}) ->
  VarCheck = get_var_selector_fun(VarSel),
  Sts = lists:map(fun(X) -> X#raws_station.id end, retrieve_stations_in_region(LatRng,LonRng)),
  case mnesia:transaction(
    fun() ->
        Q = qlc:q([X || X=#raws_obs{timestamp=T,station_id=S,var_id=V} <- mnesia:table(raws_obs),
                   T >= From, T =< To, lists:member(S,Sts), VarCheck(V)]),
        qlc:e(Q) end) of
    {atomic, R} ->
      R;
    Error ->
      Error
  end.


-spec retrieve_station_info(string()) -> #raws_station{} | no_such_station.
retrieve_station_info(StId) when is_list(StId) ->
  case mnesia:transaction(fun () -> mnesia:read({raws_station,StId}) end) of
    {atomic,[StInfo]} ->
      StInfo;
    {error,_Reason} ->
      no_such_station
  end.


-spec retrieve_station_infos([string()]) -> [#raws_station{}|no_such_station].
retrieve_station_infos(StIds) when is_list(StIds) ->
  lists:map(fun retrieve_station_info/1, StIds).


-spec retrieve_stations_in_region({number(),number()},{number(),number()}) -> [#raws_station{}].
retrieve_stations_in_region({MinLat,MaxLat},{MinLon,MaxLon}) when is_number(MinLat) and is_number(MaxLat)
                                                              and is_number(MinLon) and is_number(MaxLon) ->
  case mnesia:transaction(fun() ->
        Q = qlc:q([X || X=#raws_station{lat=Lat,lon=Lon} <- mnesia:table(raws_station),
                    Lat >= MinLat, Lat =< MaxLat, Lon >= MinLon, Lon =< MaxLon]),
        qlc:e(Q) end) of
    {atomic, R} ->
      R;
    Error ->
      Error
  end.


acquire_stations_in_region({MinLat,MaxLat},{MinLon,MaxLon},WithVars)
    when is_number(MinLat) and is_number(MaxLat)
    and  is_number(MinLon) and is_number(MaxLon)
    and is_list(WithVars) ->
  raws_ingest_server:acquire_stations_in_region({MinLat,MaxLat},{MinLon,MaxLon},WithVars).

%%-------------------------------------------------
%% Internal functions
%%-------------------------------------------------

-spec get_var_selector_fun(var_selector()) -> fun((var_id()) -> boolean()).
get_var_selector_fun(all_vars) ->
  fun (_) -> true end;
get_var_selector_fun(VarIds) when is_list(VarIds) ->
  fun (V) -> lists:member(V,VarIds) end.

