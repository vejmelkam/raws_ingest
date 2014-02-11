
-record(raws_obs,{
  timestamp :: calendar:datetime(),  % always in GMT
  station_id :: string(),
  varname :: atom(),
  value :: number()}).


-record(raws_station,{
    id :: string(),
    name :: string(),
    lat :: number(),
    lon :: number(),
    elevation :: number()}).





