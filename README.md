# raws_ingest

Erlang application that ingests RAWS observations for given variables from the MesoWest website.

## Variables
Currently the following variables are supported (the ingest software transforms them into the units given below):

MesoWest Var | raws\_ingest id | description               | units
-------------|-----------------|---------------------------|------
FM           | fm10            | 10-hr fuel moisture       | fraction (g/g)
RELH         | rel\_humidity   | relative humidity         | fraction
PREC         | accum\_precip   | accumulated precipitation | mm
TMPF         | temp            | surf. air temperature     | K
TSOI         | soil\_temp      | soil temperature          | K
PRES         | pressure        | surface pressure          | Pa

It is simple to add support for more variables if this is needed, see how in (src/mesowest_wisdom.erl).

## Using raws_ingest

Use of the `raws_ingest` application is simple.  The configuration is stored in the .app file as follows:


    {mod,{raws_ingest_app,[["ESPC2","BAWC2","RRAC2"], [temp,fm10,rel_humidity,accum_precip], 180]}}

The argument list contains a list of station ids, a list of variables and refresh frequency given as an interval in minutes.
Note that the system will not accept a refresh frequency less than 20 minutes.

As soon as the application is started, the observations for all stations are retrieved and stored periodically according to the
specified timeout.

## Storage

The observations and station information retrieved from MesoWest is stored in two PostgreSQL tables `raws_observations` and `raws_stations`
in the form of the respective records.

Any single observation is stored (in denormalized form) as a `#raws_obs` record:

    -record(raws_obs,{
      timestamp :: calendar:datetime(),
      station_id :: string(),
      lat :: number(),
      lon :: number(),
      elevation :: number(),
      var_id :: var_id(),
      value :: number(),
      variance :: number()}).

where `var_id()` is the variable id from the table above.

Information about each station is stored as the record:

    -record(raws_station,{
        id :: string(),
        name :: string(),
        lat :: number(),
        lon :: number(),
        elevation :: number()}).

A user can of course query these tables directly or use the API provided in `raws_ingest`.

## Retrieving observations from database

Observations can be retrieved using the function

    raws_ingest:retrieve_observations(SSel :: station_selector(), VarIds :: var_selector(), {From :: calendar:datetime(), To :: calendar:datetime()})

which retrieves all observations in the time interval for the selected stations and variables between From and To.

A station selector can be either `{station_list, [Code1, Code2, ...]}` or `{region, {MinLat,MaxLat},{MinLon,MaxLon}}`.


## Retrieving station information from database

The function

    raws_ingest:retrieve_stations(station_selector()) -> [#raws_station{}]

returns all stations satisfying the selector.

## Acquiring new stations and observations


The function

    raws_ingest:acquire_stations(station_selector(),var_selector()) -> [#raws_station{}]|{error,any()}.

will use remote services to resolve the station selector (instead of relying on the local database).  Any results returned
will be stored in the `raws_station` table.

The function

    raws_ingest:acquire_observations(SSel :: station_selector(), Vars ::[atom()],{From :: calendar:datetime(), To :: calendar:datetime()}, TimeoutS) -> 
      [#raws_obs{}]|{error,any()}.

will query remote services to acquire observations for stations corresponding to the station selector and for variables Vars in the
indicated interval. Information about the stations is refreshed every time observations are being acquired.  This is done because occasionally the stations move and the observations are tagged with the station locations in the `raws_observations` table.


## Forcing updates and error handling

An update can be forced with

    raws_ingest:update_now()

please be considerate in your use of the MesoWest website and do not query it too frequently.

Any errors that occurred during retrieval or parsing of the observations can be examined with

    raws_ingest:report_errors()

and cleared with

    raws_ingest:clear_errors()


# Caveats

  * if a station does not have a sensor with a variable, the MesoWest website will return nothing, the library currently does nothing about this

