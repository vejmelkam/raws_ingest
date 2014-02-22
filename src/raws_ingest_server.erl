-module(raws_ingest_server).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/5]).
-export([report_errors/0,clear_errors/0,update_now/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Token,StationIds,VarIds,TimeoutMins,Method) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Token,StationIds,VarIds,TimeoutMins,Method,[]], []).


report_errors() ->
  gen_server:call(?SERVER,report_errors).

clear_errors() ->
  gen_server:call(?SERVER,clear_errors).

update_now() ->
  gen_server:call(?SERVER,update_now).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    ?SERVER ! update_timeout,
    {ok, Args}.


handle_call(Request, _From, State=[Token,StationIds,VarIds,TimeoutMins,Method,Errors]) ->
  case Request of
    update_now ->
      TimeNow = calendar:universal_time(),
      UpdateFrom = shift_by_minutes(TimeNow, -TimeoutMins),
      {NewErrors,StationInfos,Obs} = safe_retrieve_observations(Method,Token,UpdateFrom,TimeNow,StationIds,VarIds),
      store_station_infos(StationInfos),
      store_observations(Obs),
      {reply, ok, [Token,StationIds,VarIds,TimeoutMins,Method,NewErrors ++ Errors]};
    report_errors ->
      {reply, Errors, State};
    clear_errors ->
      {reply, ok, [Token,StationIds,VarIds,TimeoutMins,Method,[]]}
  end.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(update_timeout,[Token,StationIds,VarIds,TimeoutMins,Method,Errors]) ->
  TimeNow = calendar:universal_time(),
  UpdateFrom = shift_by_minutes(TimeNow, -TimeoutMins),
  {NewErrors,StationInfos,Obs} = safe_retrieve_observations(Method,Token,UpdateFrom,TimeNow,StationIds,VarIds),
  store_station_infos(StationInfos),
  store_observations(Obs),
  timer:send_after(TimeoutMins * 60 * 1000, update_timeout),
  {noreply, [Token,StationIds,VarIds,TimeoutMins,Method,NewErrors ++ Errors]};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

store_station_infos(StInfos) ->
  mnesia:transaction(fun () -> lists:map(fun mnesia:write/1, StInfos) end).

store_observations(Obs) ->
  mnesia:transaction(fun () -> lists:map(fun mnesia:write/1, Obs) end).

safe_retrieve_observations(mesowest_json,Token,From,To,Sts,VarIds) ->
  try
    {StInfos,Obss} = mesowest_json_ingest:retrieve_observations(From,To,Sts,VarIds,Token),
    {[],StInfos,Obss}
  catch Cls:Exc ->
    error_logger:error_msg("raws_ingest_server encountered exception ~p:~p in retrieve_observations for stations ~w from ~w to ~w for vars ~w~nstacktrace: ~p",
                           [Cls,Exc,Sts,From,To,VarIds,erlang:get_stacktrace()]),
    {[{error,Sts,calendar:local_time(),Cls,Exc}], [], []}
  end;
safe_retrieve_observations(mesowest_web,_Token,_From,_To,_Sts,_VarIds) ->
  {[],[],[]}.


-spec shift_by_minutes(calendar:datetime(),integer()) -> calendar:datetime().
shift_by_minutes(DT,Mins) ->
  S = calendar:datetime_to_gregorian_seconds(DT),
  calendar:gregorian_seconds_to_datetime(S + Mins * 60).

