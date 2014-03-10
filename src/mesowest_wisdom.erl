-module(mesowest_wisdom).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([classify_varname/1,varid_to_web_name/1,varid_to_json_name/1,varid_to_unit/1,xform_value/2,estimate_variance/3,is_known_var/1]).
-include("raws_ingest.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% ----------------------------------
%% To add a new variable
%%
%% 1) make up a new atom that will identify this variable (the id)
%% 2) add a clause to classify_varname/1 and to varid_to_web_name/1 to map its name to the id
%% 3) if a transformation is required (to the physical unit of your choice), add a new clause to xform_value/2
%% 4) add a clause to varid_to_unit/1 so that its clear to the user what physical unit the variable is stored in
%% 5) add a clause to estimate_variance/3 that either returns your variance estimate for
%%    a particular observation or the atom unknown if you have no idea
%% 6) add the atom to the var_id() type in raws_ingest.hrl
%%
%% ----------------------------------


-spec is_known_var(atom()) -> boolean().
is_known_var(V) ->
  varid_to_json_name(V) /= unknown.


-spec classify_varname(string()) -> var_id()|unknown.
classify_varname("FM") -> fm10;
classify_varname("fuel_moisture_ten_hour") -> fm10;
classify_varname("FT") -> fuel_temp;
classify_varname("fuel_temp") -> fuel_temp;
classify_varname("TMPF") -> air_temp;
classify_varname("air_temp") -> air_temp;
classify_varname("RELH") -> rel_humidity;
classify_varname("relative_humidity") -> rel_humidity;
classify_varname("TSOI") -> soil_temp;
classify_varname("soil_temp") -> soil_temp;
classify_varname("PRES") -> pressure;
classify_varname("pressure") -> pressure;
classify_varname("PREC") -> accum_precip;
classify_varname("precip_accum") -> accum_precip;
classify_varname(_X) -> unknown.

-spec varid_to_web_name(var_id()) -> string().
varid_to_web_name(fm10) -> "FM";
varid_to_web_name(fuel_temp) -> "FT";
varid_to_web_name(air_temp) -> "TMPF";
varid_to_web_name(rel_humidity) -> "RELH";
varid_to_web_name(soil_temp) -> "TSOI";
varid_to_web_name(pressure) -> "PRES";
varid_to_web_name(accum_precip) -> "PREC";
varid_to_web_name(_) -> unknown.

-spec varid_to_json_name(var_id()) -> string().
varid_to_json_name(fm10) -> "fuel_moisture_ten_hour";
varid_to_json_name(fuel_temp) -> "fuel_temp";
varid_to_json_name(air_temp) -> "air_temp";
varid_to_json_name(rel_humidity) -> "relative_humidity";
varid_to_json_name(soil_temp) -> "soil_temp";
varid_to_json_name(pressure) -> "pressure";
varid_to_json_name(accum_precip) -> "precip_accum";
varid_to_json_name(_) -> unknown.

-spec varid_to_unit(var_id()) -> string().
varid_to_unit(fm10) -> "fraction";
varid_to_unit(fuel_temp) -> "K";
varid_to_unit(rel_humidity) -> "fraction";
varid_to_unit(soil_temp) -> "K";
varid_to_unit(air_temp) -> "K";
varid_to_unit(pressure) -> "Pa";
varid_to_unit(accum_precip) -> "mm".


%% The xform_value/2 function assumes that data is retrieved in English units!
%% information here comes from: http://mesowest.utah.edu/cgi-bin/droman/variable_units_select.cgi?unit=1

-spec xform_value(var_id(),number()) -> number().
xform_value(air_temp,V) when is_number(V) -> (V - 32.0) * 5.0/9.0 + 273.15;    % Fahrenheit -> Kelvin
xform_value(soil_temp,V) when is_number(V) -> (V - 32.0) * 5.0/9.0 + 273.15;   % Fahrenheit -> Kelvin
xform_value(fuel_temp,V) when is_number(V) -> (V - 32.0) * 5.0/9.0 + 273.15;   % Fahrenheit -> Kelvin
xform_value(rel_humidity,V) when is_number(V) -> 0.01 * V;                     % percent -> proportion
xform_value(fm10,V) when is_number(V) -> 0.01 * V;                             % g/100g -> g/1g
xform_value(pressure,V) when is_number(V) -> 100.0 * V;                        % mbar -> Pa
xform_value(accum_precip,V) when is_number(V) -> 25.4 * V;                     % inches -> mm
xform_value(Unknown,V) when is_number(V) -> throw({unknown_variable,Unknown,V});
xform_value(VarId,V) -> throw({not_a_number,VarId,V}).


%% Variances are associated with values that have been transformed via xform_value/2
-spec estimate_variance(string(),var_id(),number()) -> number().

% this estimate from values included from a manual Craig Clements sent by e-mail for his RAWS
% hopefully this valid for the same type of sensor in different RAWS?
% between-RAWS varability is not clear
estimate_variance(_StId,fm10,V) when V < 0.1 -> 0.0001;
estimate_variance(_StId,fm10,V) when V < 0.2 -> 0.0004;
estimate_variance(_StId,fm10,V) when V < 0.3 -> 0.0009;
estimate_variance(_StId,fm10,_V) -> 0.0016;

% best guess at variance of sensor observations that seems reasonable [more like subjective belief at this point]
estimate_variance(_StId,air_temp,_V) -> 0.04;
estimate_variance(_StId,rel_humidity,_V) -> 0.04;
estimate_variance(_StId,soil_temp,_V) -> 0.25;

% in this section, I simply have no idea what the variance is
estimate_variance(_StId,accum_precip,_V) -> unknown.

-ifdef(TEST).
-endif.

