
-module(raws_export).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([obs_to_csv/2,obs_to_csv/3]).
-export([export_station_obs_year/3,export_station_obs_csv/4,export_station_info/1]).
-export([export_station_fm_vars_csv/3]).
-export([stations_to_kml/2]).
-include("raws_ingest.hrl").


-spec obs_to_csv([#raws_obs{}],string(),string()) -> ok|{error,term()}.
obs_to_csv(Obss,Sep,Path) ->
  Str = obs_to_csv_string(Obss,Sep,[]),
  file:write_file(Path,Str).

-spec obs_to_csv([#raws_obs{}],string()) -> ok|{error,term()}.
obs_to_csv(Obss,Path) ->
  obs_to_csv(Obss,",",Path).

-spec station_to_csv_string(#raws_station{}) -> string().
station_to_csv_string(#raws_station{lat=Lat,lon=Lon,elevation=E}) ->
  LatS = item_to_list(Lat),
  LonS = item_to_list(Lon),
  ES = item_to_list(E),
  string:join([LatS,LonS,ES],",").

-spec export_station_info(#raws_station{}) -> ok|{error,term()}.
export_station_info(StId) ->
  [S] = mnesia:dirty_read(raws_station,StId),
  file:write_file(lists:flatten([StId,"_data.csv"]),station_to_csv_string(S)).

export_station_obs_year(StId,Vars,Yr) ->
  YrS = integer_to_list(Yr),
  lists:map(fun (V) ->
        Os = raws_ingest:retrieve_observations({station_list,[StId]},[V],{{{Yr,1,1},{0,0,0}},{{Yr+1,1,1},{0,0,0}}}),
        obs_to_csv(Os,lists:flatten([StId,"_",atom_to_list(V),"_",YrS,".csv"])) end, Vars).


export_station_fm_vars_csv(StId,Interval,Path) ->
  export_station_obs_csv(StId,[air_temp,rel_humidity,accum_precip,fm10],Interval,Path).


export_station_obs_csv(StId,Vars,{From,To},Path) ->
  All = raws_ingest:retrieve_observations({station_list,[StId]},Vars,{From,To}),
  Times = lists:usort(lists:map(fun (#raws_obs{timestamp=TS}) -> TS end, All)),
  file:write_file(Path,string:join(extract_rows(All,Times,[]),"\n")).


stations_to_kml(Sinfos,Path) ->
  KML = [
    "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n",
    "<kml xmlns=\"http://earth.google.com/kml/2.1\">\n",
    "<Document>\n",
    "  <name>Stations exported from raws_ingest</name>\n",
    "<Folder>\n",
    "<name>RAWS</name>\n",
    lists:map(fun station_info_to_kml/1, Sinfos),
    "</Folder>\n",
    "</Document>\n",
    "</kml>\n"],
  file:write_file(Path,lists:flatten(KML)).


% TODO: add GeoJSON export code
stations_to_geojson(Sinfos,Path) ->
  file:write_file(Path,[]).


%% ----------------------------------------
%% Internal functions
%% ----------------------------------------

-spec station_info_to_kml(#raws_station{}) -> [string()].
station_info_to_kml(#raws_station{id=I,name=N,lat=Lat,lon=Lon,elevation=Elev}) ->
  [LonS,LatS,ElevS] = lists:map(fun (X) -> io_lib:format("~p", [X]) end, [Lon,Lat,Elev]),
  [ "<Placemark>\n",
    "  <name>",I,"</name>\n",
    "  <Point><coordinates>",LonS,",",LatS,",0</coordinates></Point>\n",
    "  <description><![CDATA[ <b>Name:</b> ",N,"<br><b>Lat/Lon:</b>",LatS,",",LonS,"<br><b>Elevation:</b>",ElevS,"]]></description>\n"
    "</Placemark>\n"].


-spec obs_to_csv_string([#raws_obs{}],string(),[string()]) -> string().
obs_to_csv_string([],_,Lines) ->
  string:join(lists:reverse(Lines),"\n");
obs_to_csv_string([O|Rest],Sep,Lines) ->
  #raws_obs{timestamp={{Y,M,D},{H,Min,S}},lat=Lat,lon=Lon,value=Val,variance=Var} = O,
  StrList = lists:map(fun item_to_list/1, [Y,M,D,H,Min,S,Lat,Lon,Val,Var]),
  obs_to_csv_string(Rest,Sep,[lists:flatten(string:join(StrList, Sep))|Lines]).


-spec item_to_list(term()) -> string().
item_to_list(N) when is_integer(N) ->
  integer_to_list(N);
item_to_list(F) when is_float(F) ->
  float_to_list(F);
item_to_list(S) when is_list(S) ->
  io_lib:format("~s", [S]);
item_to_list(A) when is_atom(A) ->
  atom_to_list(A).


extract_rows([],[],Res) ->
  lists:reverse(Res);
extract_rows(Os0,[Now|Later],Res) ->
  {Cur,Os1} = lists:partition(fun(#raws_obs{timestamp=TS}) -> TS==Now end, Os0),
  Vals = lists:map(fun(#raws_obs{var_id=I,value=Val}) -> {I,Val} end, Cur),
  {{Y,M,D},{H,Min,S}} = Now,
  % extract all variables needed to run moisture model
  FMVals = lists:map(fun (V) -> proplists:get_value(V,Vals,empty) end, [air_temp,rel_humidity,accum_precip,fm10]),
  Row = string:join(lists:map(fun value_or_empty/1, [Y,M,D,H,Min,S|FMVals]), ","),
  extract_rows(Os1,Later,[Row|Res]).


value_or_empty(empty) -> "";
value_or_empty(V) when is_number(V) -> item_to_list(V).
