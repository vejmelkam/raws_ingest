-module(raws_ingest_app).

-behaviour(application).

-include("raws_ingest.hrl").

%% Application callbacks
-export([start/2, stop/1,read_config/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, Args=[StationSel,VarIds,TimeoutMins,Method]) ->
  Cfg = read_config(),
  Db = proplists:get_value(dbase, Cfg),
  User = proplists:get_value(user, Cfg),
  Pass = proplists:get_value(password, Cfg),
  pgsql_manager:start_link(Db,User,Pass,5),
  check_station_selector(StationSel),
  true = lists:foldl(fun (X,Acc) -> mesowest_wisdom:is_known_var(X) and Acc end, true, VarIds),
  check_timeout_mins(TimeoutMins),
  check_method(Method),
  raws_ingest_sup:start_link([Cfg|Args]).


stop(_State) ->
  ok.


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec check_station_selector(term()) -> ok.
check_station_selector(Sel) ->
  case raws_ingest:is_station_selector(Sel) of
    true ->
      ok;
    false ->
      error_logger:error_msg("Invalid stations description ~p, must be one of {station_list, Lst} or {region, {MinLat,MaxLat},{MinLon,MaxLon}}.~n", [Sel]),
      throw({invalid_stations, Sel})
  end.


-spec check_timeout_mins(number()) -> ok.
check_timeout_mins(T) when T >= 15 -> ok;
check_timeout_mins(_) -> throw(timeout_too_small).


check_method(mesowest_json) -> ok;
check_method(_) -> 
  error_logger:error_msg("Retrieval method must be mesowest_json.~n"),
  throw(unknown_method).


read_config() ->
  {ok, B} = file:read_file("etc/raws_config"),
  case erl_scan:string(binary_to_list(B)) of
    {ok, S, _} ->
      case erl_parse:parse_exprs(S) of
        {ok, P} ->
          case erl_eval:exprs(P, erl_eval:new_bindings()) of
            {value, V, _} ->
              V
          end
      end
  end.
    
