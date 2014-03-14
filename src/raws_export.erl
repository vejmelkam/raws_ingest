
-module(raws_export).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([obs_to_csv/2,obs_to_csv/3,export_station_data/3]).
-include("raws_ingest.hrl").


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


export_station_data(StId,Vars,Yr) ->
  YrS = integer_to_list(Yr),
  lists:map(fun (V) ->
        Os = raws_ingest:retrieve_observations({station_list,[StId]},[V],{{{Yr,1,1},{0,0,0}},{{Yr+1,1,1},{0,0,0}}}),
        obs_to_csv(Os,lists:flatten([StId,"_",atom_to_list(V),"_",YrS,".csv"])) end, Vars).


%% ----------------------------------------
%% Internal functions
%% ----------------------------------------


item_to_list(N) when is_integer(N) ->
  integer_to_list(N);
item_to_list(F) when is_float(F) ->
  float_to_list(F);
item_to_list(S) when is_list(S) ->
  io_lib:format("~s", [S]);
item_to_list(A) when is_atom(A) ->
  atom_to_list(A).

