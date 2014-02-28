-module(raws_ingest_app).

-behaviour(application).

-include("raws_ingest.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, Args=[_,VarIds,TimeoutMins,Method]) ->
    init_raws_tables(),
    Token = read_token(),
    true = lists:foldl(fun (X,Acc) -> mesowest_wisdom:is_known_var(X) and Acc end, true, VarIds),
    check_timeout_mins(TimeoutMins),
    check_method(Method),
    raws_ingest_sup:start_link([Token|Args]).

stop(_State) ->
    ok.


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------


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
  {ok,B} = file:read_file("etc/raws_tokens"),
  string:strip(binary_to_list(B),right,$\n).


check_method(mesowest_json) -> ok;
check_method(mesowest_web) -> ok;
check_method(_) -> throw(unknown_method).

