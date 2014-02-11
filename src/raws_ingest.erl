-module(raws_ingest).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([start/0]).
-export([report_errors/0,clear_errors/0,update_now/0]).
-export([station_info/1,stations_in_region/2]).
-export([retrieve_observations/2,retrieve_observations/3]).
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

-spec retrieve_observations(string(),{calendar:datetime(),calendar:datetime()}) -> [#raws_obs{}].
retrieve_observations(StId,{From,To}) ->
  case mnesia:transaction(
    fun() ->
        Q = qlc:q([X || X=#raws_obs{timestamp=T,station_id=S} <- mnesia:table(raws_obs), S =:= StId, T >= From, T =< To]),
        qlc:e(Q) end) of
    {atomic, R} ->
      R;
    Error ->
      Error
  end.


-spec retrieve_observations({calendar:datetime(),calendar:datetime()},{number(),number()},{number(),number()}) -> [#raws_obs{}].
retrieve_observations({From,To},LatRng={_MinLat,_MaxLat},LonRng={_MinLon,_MaxLon}) ->
  Sts = lists:map(fun(X) -> X#raws_station.id end, stations_in_region(LatRng,LonRng)),
  case mnesia:transaction(
    fun() ->
        Q = qlc:q([X || X=#raws_obs{timestamp=T,station_id=S} <- mnesia:table(raws_obs), lists:member(S,Sts), T >= From, T =< To]),
        qlc:e(Q) end) of
    {atomic, R} ->
      R;
    Error ->
      Error
  end.


-spec station_info(string()) -> #raws_station{} | no_such_station.
station_info(StId) ->
  case mnesia:transaction(fun () -> mnesia:read({raws_station,StId}) end) of
    {atomic,[StInfo]} ->
      StInfo;
    {error,_Reason} ->
      no_such_station
  end.


-spec stations_in_region({number(),number()},{number(),number()}) -> [#raws_station{}].
stations_in_region({MinLat,MaxLat},{MinLon,MaxLon}) ->
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


