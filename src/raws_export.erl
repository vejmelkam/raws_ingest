
-module(raws_export).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([obs_to_csv/2,obs_to_csv/3,export_station_obs/3,export_station_data/1,write_station_list/2,stations_to_kml/2]).
-include("raws_ingest.hrl").

station_to_csv_string(#raws_station{lat=Lat,lon=Lon,elevation=E}) ->
  LatS = item_to_list(Lat),
  LonS = item_to_list(Lon),
  ES = item_to_list(E),
  string:join([LatS,LonS,ES],",").

obs_to_csv_string([],_,Lines) ->
  string:join(lists:reverse(Lines),"\n");
obs_to_csv_string([O|Rest],Sep,Lines) ->
  #raws_obs{timestamp={{Y,M,D},{H,Min,S}},value=Val,variance=Var} = O,
  StrList = lists:map(fun item_to_list/1, [Y,M,D,H,Min,S,Val,Var]),
  obs_to_csv_string(Rest,Sep,[lists:flatten(string:join(StrList, Sep))|Lines]).

obs_to_csv(Obss,Sep,Path) ->
  Str = obs_to_csv_string(Obss,Sep,[]),
  file:write_file(Path,Str).

obs_to_csv(Obss,Path) ->
  obs_to_csv(Obss,",",Path).


export_station_obs(StId,Vars,Yr) ->
  YrS = integer_to_list(Yr),
  lists:map(fun (V) ->
        Os = raws_ingest:retrieve_observations({station_list,[StId]},[V],{{{Yr,1,1},{0,0,0}},{{Yr+1,1,1},{0,0,0}}}),
        obs_to_csv(Os,lists:flatten([StId,"_",atom_to_list(V),"_",YrS,".csv"])) end, Vars).


export_station_data(StId) ->
  [S] = mnesia:dirty_read(raws_station,StId),
  file:write_file(lists:flatten([StId,"_data.csv"]),station_to_csv_string(S)).


write_station_list(Ss,Path) ->
  file:write_file(Path,string:join(Ss,"\n")).


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


%% ----------------------------------------
%% Internal functions
%% ----------------------------------------

station_info_to_kml(#raws_station{id=I,name=N,lat=Lat,lon=Lon,elevation=Elev}) ->
  [LonS,LatS,ElevS] = lists:map(fun (X) -> io_lib:format("~p", [X]) end, [Lon,Lat,Elev]),
  [ "<Placemark>\n",
    "  <name>",I,"</name>\n",
    "  <Point><coordinates>",LonS,",",LatS,",0</coordinates></Point>\n",
    "  <description><![CDATA[ <b>Name:</b> ",N,"<br><b>Lat/Lon:</b>",LatS,",",LonS,"<br><b>Elevation:</b>",ElevS,"]]></description>\n"
    "</Placemark>\n"].


item_to_list(N) when is_integer(N) ->
  integer_to_list(N);
item_to_list(F) when is_float(F) ->
  float_to_list(F);
item_to_list(S) when is_list(S) ->
  io_lib:format("~s", [S]);
item_to_list(A) when is_atom(A) ->
  atom_to_list(A).

