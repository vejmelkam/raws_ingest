-module(mesowest_ingest_csv).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([retrieve_observations/3]).
-include("raws_ingest.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-compile(export_all).

-spec retrieve_observations(string(),calendar:datetime(),[string()]) -> {#raws_station{},[#raws_obs{}]}.
retrieve_observations(StationId,TimeGMT,Vars) ->
  Url = build_download_url(StationId,TimeGMT,Vars),
  {ok,Bin} = retrieve_data(Url),
  parse_data(Bin).


-spec retrieve_data(string()) -> {ok,binary()} | {error, term()}.
retrieve_data(Url) ->
  case httpc:request(get, {Url, []}, [], [{body_format,binary}]) of
    {ok, {{_, 200, _}, _, Bdy}} ->
      {ok,Bdy};
    {ok, {{_, Other, _}, _, _}} ->
      {error, Other};
    {error, Reason} ->
      {error, Reason}
  end.


-spec build_download_url(string(),calendar:datetime(),[string()]) -> string().
build_download_url(StationId,{{Y,M,D},{H,_,_}},Vars) ->
  lists:flatten([
      "http://mesowest.utah.edu/cgi-bin/droman/meso_download_mesowest_ndb.cgi?product="
      "&stn=", StationId,
      "&unit=1&time=GMT",
      "&daycalendar=1",
      "&day1=", integer_to_list(D),
      io_lib:format("&month1=~2..0B", [M]),
      "&year1=", integer_to_list(Y),
      io_lib:format("&hour1=~2..0B", [H]),
      "&hours=24&output=csv&order=1",
      lists:map(fun (V) -> ["&",V,"=",V] end, Vars) ]).

-spec strip_tags(binary()) -> string().
strip_tags(Bin) ->
  re:replace(Bin,"<[^>]+>", "", [{return,list},global]).

-spec parse_data(binary()|string()) -> {#raws_station{},[#raws_obs{}]}.
parse_data(Bin) when is_binary(Bin) ->
  parse_data(strip_tags(Bin));
parse_data(Data) ->
  [SIText|Rest] = lists:filter(fun(Line) -> length(Line) > 10 end, string:tokens(Data, "\n")),
  % station info should be the first line containing more than 10 characters after stripping tags
  StationInfo = parse_station_info(string:tokens(SIText, " \t\n")),
  StId = StationInfo#raws_station.id,
  % keep looking for the line starting PARM =
  [Variables|ObsText] = lists:dropwhile(fun (L) -> not lists:prefix("PARM =", L) end, Rest),
  VarSeq = parse_variable_names(Variables),
  % next keep parsing all lines containing observations until end of text
  Obss = parse_observations(ObsText,VarSeq,[]),
  {StationInfo,lists:map(fun (X) -> X#raws_obs{station_id=StId} end, Obss)}.


-spec parse_variable_names(string()) -> [string()].
parse_variable_names([$P,$A,$R,$M,$ ,$=, $ |VarNames]) ->
  {_,Vars} = lists:split(6,string:tokens(VarNames,",\n")),
  Vars.

-spec parse_observations([string()],[string()],[#raws_obs{}]) -> [#raws_obs{}].
parse_observations([],_,Os) ->
  lists:flatten(Os);
parse_observations([O|Rest],VarSeq,Acc) when length(O) > 10 ->
  parse_observations(Rest,VarSeq,[parse_single_obs(O,VarSeq)|Acc]);
parse_observations([_|Rest],VarSeq,Acc) ->
  parse_observations(Rest,VarSeq,Acc).

-spec parse_single_obs(string(),[string()]) -> #raws_obs{}.
parse_single_obs(Text,VarSeq) ->
  [MonStr,DayStr,YearStr,HourStr,MinStr,"GMT"|ValueStrs] = split_on_commas(Text),
  [Mon,Day,Year,Hour,Min] = lists:map(fun list_to_integer/1, [MonStr,DayStr,YearStr,HourStr,MinStr]),
  Timestamp = {{Year,Mon,Day},{Hour,Min,0}},
  DefValsOnly = lists:filter(fun ({_,[]}) -> false; (_) -> true end, lists:zip(VarSeq,ValueStrs)),
  lists:map(fun ({Var,Val}) -> #raws_obs{timestamp=Timestamp,varname=Var,value=list_to_number(Val)} end, DefValsOnly).

-spec split_on_commas(string()) -> [string()].
split_on_commas(Text) ->
  lists:map(fun string:strip/1, split_on_commas(Text,[],[])).

split_on_commas([],CurTok,Acc) ->
  lists:reverse([lists:reverse(CurTok)|Acc]);
split_on_commas([$\n],CurTok,Acc) ->
  lists:reverse([lists:reverse(CurTok)|Acc]);
split_on_commas([$,|Rest],CurTok,Acc) ->
  split_on_commas(Rest,[],[lists:reverse(CurTok)|Acc]);
split_on_commas([Other|Rest],CurTok,Acc) ->
  split_on_commas(Rest,[Other|CurTok],Acc).

-spec parse_station_info([string()]) -> #raws_station{}.
parse_station_info([StationId|Rest]) ->
  ["RAWS", "m", ElevStr, LonStr, LatStr | NameTokens] = lists:reverse(Rest),
  Name = string:join(lists:reverse(NameTokens), " "),
  Elev = list_to_number(ElevStr),
  Lon = list_to_number(LonStr),
  Lat = list_to_number(LatStr),
  #raws_station{id=StationId,name=Name,lat=Lat,lon=Lon,elevation=Elev}.

-spec list_to_number(string()) -> float|integer.
list_to_number(Text) ->
  case string:to_float(Text) of
      {F,[]} ->
        F;
      {error,_} ->
        list_to_integer(Text)
  end.

-ifdef(TEST).

split_on_commas_empty_test() ->
  ["A","B","C",[],"5.5"] = raws_ingest_csv:split_on_commas("A,B,C,,5.5\n").
split_on_commas_trailing_comma_test() ->
  ["A",[]] = raws_ingest_csv:split_on_commas("A,\n").
split_on_commas_empty_and_trailing_test() ->
  ["A",[],[]] = raws_ingest_csv:split_on_commas("A,,\n").

list_to_number_float_test() ->
  5.321 = list_to_number("5.321").
list_to_number_int_test() ->
  4424 = list_to_number("4424").

parse_station_info_espc2_test() ->
  #raws_station{id="ESPC2",name="ESTES PARK",lat=40.366361,lon=-105.562778,elevation=2405} = parse_station_info(string:tokens("ESPC2 ESTES PARK 40.366361 -105.562778 2405 m RAWS\n", " \t\n")).

parse_variable_names_test() ->
  ["TMPF","RELH","SKNT","GUST","DRCT","QFLG","SOLR","TLKE","PREC","SINT","FT","FM","PEAK","PDIR","VOLT","DWPF"] = 
  parse_variable_names("PARM = MON,DAY,YEAR,HR,MIN,TMZN,TMPF,RELH,SKNT,GUST,DRCT,QFLG,SOLR,TLKE,PREC,SINT,FT,FM,PEAK,PDIR,VOLT,DWPF\n").

parse_single_obs_test() ->
  [#raws_obs{timestamp={{2014,2,10},{19,24,0}},varname="TMPF",value=3.3}] = parse_single_obs("2,10,2014, 19,24,GMT, 3.3\n", ["TMPF"]).

build_download_espc2_url() ->
  "http://mesowest.utah.edu/cgi-bin/droman/meso_download_mesowest_ndb.cgi?product=&stn=ESPC2&unit=1&time=GMT&daycalendar=1&day1=9&month1=02&year1=2014&hour1=23&hours=24&output=csv&order=1&TMPF=TMPF&FM=FM" = build_download_url("ESPC2",{{2014,2,9},{23,0,0}},["TMPF","FM"]).

-endif.

