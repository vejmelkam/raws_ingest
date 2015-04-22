
-type var_id() :: fm10|air_temp|rel_humidity|soil_temp|pressure|accum_precip|fuel_temp|wind_speed|wind_dir|wind_gust|solar_rad.
-type station_selector() :: {station_list,[string()]}|{region,{number(),number()},{number(),number()}}|{station_file,string()}|empty_station_selector.
-type var_selector() :: all_vars|[var_id()].

-record(raws_obs,{
  timestamp :: calendar:datetime(),  % always in GMT
  station_id :: string(),
  lat :: number(),
  lon :: number(),
  elevation :: number(),
  var_id :: var_id(),
  value :: number(),
  variance :: number()}).


-record(raws_station,{
    id :: string(),
    name :: string(),
    lat :: number(),   % note: lat/lon are only the CURRENT position (some RAWS are portable)
    lon :: number(),
    elevation :: number()}).


-record(raws_var,{
    id :: string(),
    full_name :: string(),
    units :: string()}).



