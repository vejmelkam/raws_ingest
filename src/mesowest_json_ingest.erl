
-module(mesowest_json_ingest).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([find_stations_in_bbox/4,retrieve_observations/5,retrieve_variables/1]).

-include("raws_ingest.hrl").

-define(MW_VAR_URL, "http://api.mesowest.net/variables").
-define(MW_STATION_URL, "http://api.mesowest.net/stations").

%% @doc Pings the Mesowest server to request a list of variables
%% @doc that are available on the server.
-spec retrieve_variables(string()) -> [#raws_var{}].
retrieve_variables(Token) ->
  Url = build_url(?MW_VAR_URL, [{"token", Token}]),
  {ok,JSONStr} = retrieve_json(Url),
  {JSON} = jiffy:decode(JSONStr),
  {Vars} = proplists:get_value(<<"VARIABLES">>,JSON),
  lists:map(fun json_to_var/1, Vars).


%% @doc Retrieve observations between times <From> and <To> for the stations
%% @doc <StationIds> and the variables <VarIds>.  Use the API token <Token>.
-spec retrieve_observations(calendar:datetime(),calendar:datetime(),[string()],[var_id()],string()) -> {[#raws_station{}],[#raws_obs{}]}.
retrieve_observations(From,To,StationIds,VarIds,Token) ->
  Url = build_url(?MW_STATION_URL,
                  [{"token",Token},
                   {"stid",string:join(StationIds,",")},
                   {"start",to_timestamp(From)},
                   {"end",to_timestamp(To)},
                   {"vars",string:join(lists:map(fun mesowest_wisdom:varid_to_json_name/1,VarIds),",")}]),
  {ok,JSONStr} = retrieve_json(Url),
  {JSON} = jiffy:decode(JSONStr),
  ok = check_summary(proplists:get_value(<<"SUMMARY">>,JSON)),
  Stations = proplists:get_value(<<"STATION">>,JSON),
  {StInfo,Obss} = lists:unzip(lists:map(fun (S) -> extract_observations(S,VarIds) end, Stations)),
  {StInfo,lists:flatten(Obss)}.


%% @doc Convert a datetime to a string representation, disregarding minutes.
-spec to_timestamp(calendar:datetime()) -> string().
to_timestamp({{Y,M,D},{H,Min,_}}) ->
  io_lib:format("~4..0B~2..0B~2..0B~2..0B~2..0B", [Y,M,D,H,Min]).


%% @doc Returns all stations in the bbox <MinLat>,<MaxLat> and <MinLon>, <MaxLon> that
%% @doc report variables <WithVars>. Uses the API token <Token>.
-spec find_stations_in_bbox({number(),number()},{number(),number()},[string()],string()) -> [#raws_station{}].
find_stations_in_bbox({MinLat,MaxLat},{MinLon,MaxLon},WithVars,Token) ->
  Bbox = io_lib:format("~p,~p,~p,~p", [MinLon,MinLat,MaxLon,MaxLat]),
  Ps = [{"network", "2"}, {"bbox", Bbox}],
  case WithVars of
    [] ->
      list_stations(Ps,Token);
    _NotEmpty ->
      list_stations([{"vars",string:join(lists:map(fun mesowest_wisdom:varid_to_json_name/1,WithVars),",")}|Ps],Token)
  end.

%% @doc Lists all stations that satisfy parameters <Params>.
-spec list_stations([{string(),string()}],string()) -> [#raws_station{}].
list_stations(Params,Token) ->
  URL = build_url(?MW_STATION_URL, [{"token",Token}|Params]),
  {ok, JSON} = retrieve_json(URL),
  {TopDict} = jiffy:decode(JSON),
  ok = check_summary(proplists:get_value(<<"SUMMARY">>,TopDict)),
  case proplists:get_value(<<"STATION">>,TopDict) of
    undefined ->
      [];
    Ss ->
      lists:map(fun json_to_station/1, Ss)
  end.


%% @doc Sends out an HTTP request to download the contents of a
%% @doc URL.
-spec retrieve_json(string()) -> {ok,binary()} | {error, any()}.
retrieve_json(Url) ->
  case httpc:request(get, {Url, []}, [], [{body_format,binary}]) of
    {ok, {{_, 200, _}, _, Bdy}} ->
      {ok,Bdy};
    {ok, {{_, Other, _}, _, _}} ->
      {error, Other};
    {error, Reason} ->
      {error, Reason}
  end.


%% @doc Construct a URL from a base address and a property list of
%% @doc parameters.
-spec build_url(string(), [{string(),string()}]) -> string().
build_url(Fixed,Params) ->
  lists:flatten([
    Fixed,
    "?",
    string:join(lists:map(fun ({P,V}) -> [P, "=", V] end, Params),"&")]).


%% @doc Decode station information in JSON into a raws_station record.
-spec json_to_station({[{binary(),binary()}]}) -> #raws_station{}.
json_to_station({S}) ->
  Name = binary_to_list(proplists:get_value(<<"NAME">>, S)),
  % note: elevation is converted from feet to meters above sea level
  Elev = case binary_to_number(proplists:get_value(<<"ELEVATION">>, S)) of
    null ->
      null;
    ElevInFeet ->
      ElevInFeet / 3.2808399
  end,
  StId = binary_to_list(proplists:get_value(<<"STID">>, S)),
  Lon = binary_to_number(proplists:get_value(<<"LONGITUDE">>, S)),
  Lat = binary_to_number(proplists:get_value(<<"LATITUDE">>, S)),
  #raws_station{id=StId,name=Name,lat=Lat,lon=Lon,elevation=Elev}.


%% @doc Decode a binary string into a number or null.
-spec binary_to_number(binary()) -> number().
binary_to_number(<<"NULL">>) -> null;
binary_to_number(null) -> null;
binary_to_number(B) when is_binary(B) ->
  L = string:strip(binary_to_list(B)),
  case string:to_integer(L) of
    {I,[]} ->
      I;
    _ ->
      {F,[]} = string:to_float(L),
      F
  end.


%% @doc Extract observations from a JSON record and store it in a tuple with
%% @doc the station info.
-spec extract_observations({term()},[atom()]) -> {term(),[#raws_obs{}]}.
extract_observations({S},VarIds) ->
  StInfo = json_to_station({S}),
  #raws_station{id=StId,lon=Lon,lat=Lat} = StInfo,
  {Obss} = proplists:get_value(<<"OBSERVATIONS">>,S),
  TS = lists:map(fun decode_datetime/1,proplists:get_value(<<"date_time">>,Obss)),
  {StInfo,lists:map(fun(VarId) -> extract_observations_for_var(VarId,StId,Lat,Lon,TS,Obss) end, VarIds)}.


extract_observations_for_var(VarId,StId,Lat,Lon,TS,S) ->
  Var = mesowest_wisdom:varid_to_json_name(VarId),
  case proplists:get_value(list_to_binary(Var), S) of
    undefined ->
      [];
    Vals ->
      lists:map(fun ({T,V}) when is_number(V) ->
        #raws_obs{station_id=StId,lat=Lat,lon=Lon,var_id=VarId,timestamp=T,
                  value=mesowest_wisdom:xform_value(VarId,V),
                  variance=mesowest_wisdom:estimate_variance(StId,VarId,V)};
              ({_,_}) -> []
          end, lists:zip(TS,Vals))
  end.


%% @doc Decode a datetime stored as YYYY-MM-DD HH:MM in a binary into
%% @doc an erlang datetime.
-spec decode_datetime(binary()) -> calendar:datetime().
decode_datetime(<<Y1,Y2,Y3,Y4,$-,M1,M2,$-,D1,D2,$ ,H1,H2,$:,Mi1,Mi2>>) ->
  {{list_to_integer([Y1,Y2,Y3,Y4]), list_to_integer([M1,M2]), list_to_integer([D1,D2])},
    {list_to_integer([H1,H2]), list_to_integer([Mi1,Mi2]), 0}}.


%% @doc Convert a JSON record of a varialble into a variable record.
json_to_var({VarNameB,{Info}}) ->
  FullName = binary_to_list(proplists:get_value(<<"long_name">>,Info)),
  Units = binary_to_list(proplists:get_value(<<"unit">>,Info)),
  #raws_var{id=binary_to_list(VarNameB),full_name=FullName,units=Units}.


%% @doc Checks if the response code is 1.
-spec check_summary({any()}) -> ok|{error, number()}.
check_summary({S}) ->
  case proplists:get_value(<<"RESPONSE_CODE">>, S) of
    1 ->
      ok;
    Other ->
      {error, Other}
  end.
