-module(raws_ingest_app).

-behaviour(application).

-include("raws_ingest.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, Args=[StationSel,VarIds,TimeoutMins,Method]) ->
    init_raws_tables(),
    Token = read_token(),
    check_station_selector(StationSel),
    true = lists:foldl(fun (X,Acc) -> mesowest_wisdom:is_known_var(X) and Acc end, true, VarIds),
    check_timeout_mins(TimeoutMins),
    check_method(Method),
    raws_ingest_sup:start_link([Token|Args]).

stop(_State) ->
    ok.


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec check_station_selector(term()) -> ok.
check_station_selector(Sel) ->
  case raws_ingest_server:is_station_selector(Sel) of
    true ->
      ok;
    false ->
      error_logger:error_msg("Invalid stations description ~p, must be one of {station_list, Lst} or {region, {MinLat,MaxLat},{MinLon,MaxLon}}.", [Sel]),
      throw({invalid_stations, Sel})
  end.


-spec init_raws_tables() -> ok.
init_raws_tables() ->
  ensure_table_exists(raws_station,record_info(fields,raws_station),[lat,lon]),
  ensure_table_exists(raws_obs,record_info(fields,raws_obs),[station_id,var_id]),
  ok.


-spec ensure_table_exists(atom(),[atom()], [atom()]) -> ok.
ensure_table_exists(Name,RecFields,NdxFields) ->
  case lists:member(Name,mnesia:system_info(tables)) of
    true ->
      ok;
    false ->
      {atomic,ok} = mnesia:create_table(Name, [{attributes,RecFields}, {disc_copies,[node()]}, {index,NdxFields}, {type,bag}]),
      ok
  end.


-spec check_timeout_mins(number()) -> ok.
check_timeout_mins(T) when T >= 60 -> ok;
check_timeout_mins(_) -> throw(timeout_too_small).


read_token() ->
  case file:read_file("etc/raws_tokens") of
    {ok,B} ->
      string:strip(binary_to_list(B),right,$\n);
    {error, Reason} ->
      error_logger:error_msg("Failed to read API token from etc/raws_tokens with reason ~p", [Reason]),
      throw({no_token, Reason})
  end.


check_method(mesowest_json) -> ok;
check_method(mesowest_web) -> ok;
check_method(_) -> 
  error_logger:error_msg("Retrieval method must be either mesowest_json or mesowest_web."),
  throw(unknown_method).

