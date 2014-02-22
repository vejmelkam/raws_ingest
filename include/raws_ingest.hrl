
-type var_id() :: fm10|air_temp|rel_humidity|soil_temp|pressure|accum_precip|fuel_temp.

-record(raws_obs,{
  timestamp :: calendar:datetime(),  % always in GMT
  station_id :: string(),
  var_id :: var_id(),
  value :: number(),
  variance :: number()}).


-record(raws_station,{
    id :: string(),
    name :: string(),
    lat :: number(),
    lon :: number(),
    elevation :: number()}).


-record(raws_var,{
    id :: string(),
    full_name :: string(),
    units :: string()}).



