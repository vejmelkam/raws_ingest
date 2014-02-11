
-type var_id() :: fm10|temp|rel_humidity|skin_temp|pressure.

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





