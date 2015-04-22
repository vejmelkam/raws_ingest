-module(raws_sql).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-include("raws_ingest.hrl").
-export([store_raws_obs/1, sql_to_raws_obs/1, raws_obs_to_sql/1]).
-export([retrieve_observations/3]).
-export([store_raws_station/1, sql_to_raws_station/1, raws_station_to_sql/1]).
-export([retrieve_stations/1]).


%% ----------------------------------------
%% Store/retrieve raws stations
%% ----------------------------------------


-spec sql_to_raws_station({binary(),binary(),number(),number(),number()}) -> #raws_station{}.
sql_to_raws_station({CodeB,NameB,Lat,Lon,Elev}) ->
  Code = binary_to_list(CodeB),
  Name = binary_to_list(NameB),
  #raws_station{id=Code,name=Name,lat=Lat,lon=Lon,elevation=Elev}.


%% @doc Convert a raws_station record into a list suitable for use with pgsql insert operations.
-spec raws_station_to_sql(#raws_station{}) -> [term()].
raws_station_to_sql(#raws_station{id=Code,name=Name,lon=Lon,lat=Lat,elevation=Elev}) ->
  [Code,Name,Lat,Lon,Elev].

%% @doc Store station information in the raws_station table.
-spec store_raws_station(#raws_station{}) -> ok|error.
store_raws_station(St=#raws_station{}) ->
  Sql = raws_station_to_sql(St),
  QryU = "update raws_stations set name=$2,lat=$3,lon=$4,elevation=$5 where raws_code=$1",
  case pgsql_manager:extended_query(QryU, Sql) of
    {{update,1},_} ->
      ok;
    {{update,0},[]} ->
      QryI = "insert into raws_stations(raws_code,name,lat,lon,elevation) values($1,$2,$3,$4,$5)",
      case pgsql_manager:extended_query(QryI, raws_station_to_sql(St)) of
        {{insert,0,1},[]} ->
          ok;
        {error, E} ->
          error_logger:error_msg("Failed inserting ~p into table raws_stations with error ~p", [Sql,E]),
          error
      end;
    {error, E} ->
      error_logger:error_msg("Failed updating table raws_station with ~p with error ~p", [Sql,E]),
      error
  end.


%% @doc Retrieves information for one station.
-spec retrieve_stations(station_selector()) -> [#raws_station{}].
retrieve_stations({region,{MinLat,MaxLat},{MinLon,MaxLon}}) ->
  Qry = "select raws_code,name,lat,lon,elevation from raws_stations where lat >= $1 and lat <= $2 and lon >= $3 and lon <= $4",
  case pgsql_manager:extended_query(Qry, [MinLat,MaxLat,MinLon,MaxLon]) of
    {{select, _N}, Data} ->
      lists:map(fun sql_to_raws_station/1, Data)
  end;
retrieve_stations({station_list,StIds}) ->
  Qry = "select raws_code,name,lat,lon,elevation from raws_stations where raws_code in ",
  InSt = list_to_sql_in(StIds),
  case pgsql_manager:simple_query(Qry ++ InSt) of
    {{select, _N}, Data} ->
      lists:map(fun sql_to_raws_station/1, Data)
  end.

 
%% ----------------------------------------
%% Store/Retrieve raws observations
%% ----------------------------------------


%% @doc Store a single observation in the raws_observations table.
-spec store_raws_obs(#raws_obs{}) -> ok|error.
store_raws_obs(Obs=#raws_obs{}) ->
  Sql = raws_obs_to_sql(Obs),
  QryU = "update raws_observations set lat=$3,lon=$4,elevation=$5,value=$7,variance=$8 where ts=$1 and raws_code=$2 and var_id=$6",
  case pgsql_manager:extended_query(QryU, Sql) of
    {{update, 1}, _} ->
      ok;
    {{update, 0}, []} ->
      QryI = "insert into raws_observations(ts,raws_code,lat,lon,elevation,var_id,value,variance) values($1,$2,$3,$4,$5,$6,$7,$8)",
      case pgsql_manager:extended_query(QryI, Sql) of
        {{insert,0,1},_}
          -> ok;
        {error, E} ->
          error_logger:error_msg("Failed inserting ~p into table raws_observations with error ~p", [Obs,E]),
          error
      end;
    {error, E} ->
      error_logger:error_msg("Failed updating ~p in table raws_observations with error ~p", [Obs,E]),
      error
  end.


-spec raws_obs_to_sql(#raws_obs{}) -> [term()].
raws_obs_to_sql(#raws_obs{timestamp=TS,station_id=Sid,lat=Lat,lon=Lon,elevation=Elev,var_id=VarId,value=V,variance=Var0}) ->
  % substitute unknown variances with -1 for SQL storage
  Var = case Var0 of
    unknown -> -1;
    Number -> Number
  end,
  [TS,Sid,Lat,Lon,Elev,atom_to_list(VarId),V,Var].


-spec sql_to_raws_obs([term()]) -> #raws_obs{}.
sql_to_raws_obs({TS,SidB,Lat,Lon,Elev,VarIdB,V,Var0}) ->
  Var = case Var0 of
    -1 -> unknown;
    Number -> Number
  end,
  #raws_obs{timestamp=TS, station_id=binary_to_list(SidB), lat=Lat, lon=Lon, elevation=Elev,
            var_id=list_to_atom(binary_to_list(VarIdB)), value=V, variance=Var}.


%% @doc Retrieve observations from the database that match the station selector, the variable selector
%% @doc and the time constraints.
-spec retrieve_observations(station_selector(),var_selector(),{calendar:datetime(),calendar:datetime()}) -> [#raws_obs{}].
retrieve_observations({region, {MinLat,MaxLat}, {MinLon,MaxLon}}, Vars, {From, To}) ->
  Qry0 = "select ts,raws_code,lat,lon,elevation,var_id,value,variance from raws_observations where ts >= $1 and ts <= $2 and"
      ++ " lat >= $3 and lat <= $4 and lon >= $5 and lon <= $6",
  Result = case Vars of
    all_vars ->
      pgsql_manager:extended_query(Qry0, [From, To, MinLat, MaxLat, MinLon, MaxLon]);
    List ->
      Qry1 = Qry0 ++ " and var_id in " ++ list_to_sql_in(List),
      pgsql_manager:extended_query(Qry1, [From, To, MinLat, MaxLat, MinLon, MaxLon])
  end,
  case Result of
    {{select, _N}, Data} ->
      lists:map(fun sql_to_raws_obs/1, Data);
    Error->
      Error
  end;
retrieve_observations(SSel, Vars, {From, To}) ->
  {station_list, SList} = raws_ingest:resolve_station_selector(SSel),
  QryS = list_to_sql_in(SList),
  Qry0 = "select ts,raws_code,lat,lon,elevation,var_id,value,variance from raws_observations where ts >= $1 and ts <= $2 and raws_code in " ++ QryS,
  Result = case Vars of
    all_vars ->
      pgsql_manager:extended_query(Qry0, [From, To]);
    List ->
      Qry1 = Qry0 ++ " and var_id in " ++ list_to_sql_in(List),
      pgsql_manager:extended_query(Qry1, [From, To])
  end,
  case Result of
    {{select, _N}, Data} ->
      lists:map(fun sql_to_raws_obs/1, Data);
    Error->
      Error
  end.



%% ----------------------------------------
%% Miscellaneous functions
%% ----------------------------------------

%% @doc Convert a list of strings or atoms into a string that can be used in an 'in' expression in SQL.
-spec list_to_sql_in([string()]) -> string().
list_to_sql_in([]) ->
    "()";
list_to_sql_in(List) when is_list(hd(List)) ->
  lists:flatten(["('", string:join(List,"','"), "')"]);
list_to_sql_in(AtomList) when is_atom(hd(AtomList))->
  list_to_sql_in(lists:map(fun atom_to_list/1, AtomList)).


