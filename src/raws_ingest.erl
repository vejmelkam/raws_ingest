-module(raws_ingest).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([start/0]).
-export([report_errors/0,clear_errors/0,update_now/0]).
-export([retrieve_station_info/1,retrieve_stations_in_region/2]).
-export([retrieve_observations/3,acquire_observations/3]).
-include("raws_ingest.hrl").
-include_lib("stdlib/include/qlc.hrl").

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


-spec acquire_observations(station_selector(),[var_id()],{calendar:datetime(),calendar:datetime()}) -> ok|{error,any()}.
acquire_observations(_SSel,_VarIds,{From,To}) when From > To ->
  ok;
acquire_observations(SSel,VarIds,{From,To}) ->
  STo = calendar:datetime_to_gregorian_seconds(To),
  SFrom = calendar:datetime_to_gregorian_seconds(From),
  case STo - SFrom > 2 * 86400 of
    true ->
      T = calendar:gregorian_seconds_to_datetime(SFrom + 2 * 86400),
      raws_ingest_server:acquire_observations(SSel,VarIds,{From,T}),
      acquire_observations(SSel,VarIds,{T,To});
    false ->
      raws_ingest_server:acquire_observations(SSel,VarIds,{From,To})
  end.


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
retrieve_station_info(StId) ->
  case mnesia:transaction(fun () -> mnesia:read({raws_station,StId}) end) of
    {atomic,[StInfo]} ->
      StInfo;
    {error,_Reason} ->
      no_such_station
  end.


-spec retrieve_stations_in_region({number(),number()},{number(),number()}) -> [#raws_station{}].
retrieve_stations_in_region({MinLat,MaxLat},{MinLon,MaxLon}) ->
  case mnesia:transaction(
    fun() ->
        Q = qlc:q([X || X=#raws_station{lat=Lat,lon=Lon} <- mnesia:table(raws_station),
                    Lat >= MinLat, Lat =< MaxLat, Lon >= MinLon, Lon =< MaxLon]),
        qlc:e(Q) end) of
    {atomic, R} ->
      R;
    Error ->
      Error
  end.


%%-------------------------------------------------
%% Internal functions
%%-------------------------------------------------


-spec get_var_selector_fun(var_selector()) -> fun((var_id()) -> boolean()).
get_var_selector_fun(all_vars) ->
  fun (_) -> true end;
get_var_selector_fun(VarIds) when is_list(VarIds) ->
  fun (V) -> lists:member(V,VarIds) end.
