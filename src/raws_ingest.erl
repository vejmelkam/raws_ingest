-module(raws_ingest).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([start/0]).
-export([report_errors/0,clear_errors/0,update_now/1]).
-export([is_station_selector/1]).
-export([resolve_station_selector/1,stations_to_ids/1]).
-export([retrieve_stations/1,acquire_stations/2]).
-export([retrieve_observations/3,acquire_observations/4]).
-include("raws_ingest.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(MAX_ACQ_INTERVAL,43200).


start() ->
  inets:start(),
  application:start(raws_ingest).


-spec report_errors() -> [{error,string(),calendar:datetime(),atom(),any()}].
report_errors() ->
  raws_ingest_server:report_errors().


-spec clear_errors() -> ok.
clear_errors() ->
  raws_ingest_server:clear_errors().

-spec stations_to_ids([#raws_station{}]) -> [string()].
stations_to_ids(Sts) ->
  lists:map(fun (#raws_station{id=Id}) -> Id end, Sts).


%% @doc Converts any station selector into the selector {station_list, Lst}.
%% @doc Queries the local database/filesystem only, no remote requests are made.
-spec resolve_station_selector(station_selector()) -> station_selector().
resolve_station_selector(empty_station_selector) ->
  empty_station_selector;
resolve_station_selector({station_list, Lst}) ->
  {station_list, Lst};
resolve_station_selector({station_file,Path}) ->
  {ok,B} = file:read_file(Path),
  {station_list, string:tokens(binary_to_list(B),"\n")};
resolve_station_selector(Sel={region, {MinLat,MaxLat}, {MinLon,MaxLon}}) ->
  try
    Stations = raws_sql:retrieve_stations_in_bbox({MinLat,MaxLat},{MinLon,MaxLon}),
    {station_list, stations_to_ids(Stations)}
  catch Bdy:Exc ->
    error_logger:error_msg("Failed to resolve station selector ~p due to exception~n~p (bdy ~p)~nstacktrace:~p~n",
                           [Sel,Bdy,Exc,erlang:get_stacktrace()]),
    Sel
  end.


%% @doc Perform a server update now.
-spec update_now(non_neg_integer()) -> ok.
update_now(TimeoutS) ->
  raws_ingest_server:update_now(TimeoutS).


-spec retrieve_observations(station_selector(),var_selector(),{calendar:datetime(),calendar:datetime()}) -> [#raws_obs{}].
retrieve_observations(SSel, Vars, {From, To}) ->
  raws_sql:retrieve_observations(SSel,Vars,{From,To}).


%% @doc Retrieve station records from a list of ids.
-spec retrieve_stations(station_selector()) -> [#raws_station{}].
retrieve_stations(SSel) ->
  true = is_station_selector(SSel),
  raws_sql:retrieve_stations(SSel).


%% @doc Make a remote HTTP request to list all stations that satisfy the selector.
%% @doc Rather than querying the database, this will populate the database with the results.
-spec acquire_stations(station_selector(),[var_id()]) -> [#raws_station{}]|{error,any()}.
acquire_stations(SSel,WithVars) ->
  true = is_station_selector(SSel),
  raws_ingest_server:acquire_stations(SSel,WithVars).


%% @doc Acquire observations for the station selector and the variables given in the interval
%% @doc From <--> To and set a timeout for the operation.
-spec acquire_observations(station_selector(),[var_id()],{calendar:datetime(),calendar:datetime()},pos_integer()) -> [#raws_obs{}]|{error,any()}.
acquire_observations(SSel,VarIds,{From,To},TimeoutS) when is_list(VarIds) and is_number(TimeoutS) ->
  acquire_observations(SSel,VarIds,{From,To},TimeoutS,[]).


%%-------------------------------------------------
%% Verification functions
%%-------------------------------------------------


%% @doc Check if a term is a valid station selector.
-spec is_station_selector(any()) -> boolean().
is_station_selector({station_list,Lst}) when is_list(Lst) ->
  true;
is_station_selector({region,{MinLat,MaxLat},{MinLon,MaxLon}}) when is_number(MinLat) and is_number(MaxLat)
                                                               and is_number(MinLon) and is_number(MaxLon) ->
  true;
is_station_selector({station_file,Path}) when is_list(Path) ->
  true;
is_station_selector(empty_station_selector) ->
  true;
is_station_selector(_) ->
  false.


%%-------------------------------------------------
%% Internal functions
%%-------------------------------------------------


%% @doc An internal function that splits the observation acquisition into different time slices
%% @doc to prevent bombarding the server with requests that are too large.
-spec acquire_observations(station_selector(),[var_id()],{calendar:datetime(),calendar:datetime()},pos_integer(),[any()]) -> [#raws_obs{}]|{error,any()}.
acquire_observations(_SSel,_VarIds,{From,To},_TimeoutS,Res) when From >= To ->
  lists:flatten(Res);
acquire_observations(SSel,VarIds,{From,To},TimeoutS,Res) when is_list(VarIds) and is_number(TimeoutS) ->
  STo = calendar:datetime_to_gregorian_seconds(To),
  SFrom = calendar:datetime_to_gregorian_seconds(From),
  Qto = case STo - SFrom > ?MAX_ACQ_INTERVAL of
    true ->
      calendar:gregorian_seconds_to_datetime(SFrom + ?MAX_ACQ_INTERVAL);
    false ->
      To
  end,
  case raws_ingest_server:acquire_observations(SSel,VarIds,{From,Qto},TimeoutS) of
    {error, Reason} ->
      {error, Reason};
    Os ->
      acquire_observations(SSel,VarIds,{Qto,To},TimeoutS,[Os|Res])
  end.

