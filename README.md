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
Note that the system will not accept a refresh frequency less than 60 minutes.

As soon as the application is started, the observations for all stations are retrieved and stored periodically according to the
specified timeout.

## Storage

The observations and station information retrieved from MesoWest is stored in two mnesia tables `raws_obs` and `raws_station`
in the form of the respective records.

Any single observation is stored as a `#raws_obs` record:

    -record(raws_obs,{
      timestamp :: calendar:datetime(),
      station_id :: string(),
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

## Retrieving observations

Observations can be retrieved using two functions, either

    raws_ingest:retrieve_observations(StationId :: string(), {From :: calendar:datetime(), To :: calendar:datetime()})

which retrieves all observations in the time interval for the station (of all variables that are stored).

Alternatively, to retrieve all observations stored in a rectangular lon/lat region, one can use:


    raws_ingest:retrieve_observations({From :: calendar:datetime(), To :: calendar:datetime()},
                                      {MinLat :: number(), MaxLat :: number()},
                                      {MinLon :: number(), MaxLon :: number()})

which will retrieve all observations acquired in this geographic region and time interval.


## Retrieving station information

The function

    raws_ingest:station_info(StationId :: string())

returns either a `#raws_station` record or `no_such_station` if no station with the given id exists in the mnesia table.

The function

    raws_ingest:stations_in_region({MinLat :: number(),MaxLat :: number()},{MinLon :: number(), MaxLon :: number()})

returns all the stations with lon/lat coordinates within the given ranges.


## Forcing updates and error handling

An update can be forced with

    raws_ingest:update_now()

please be considerate in your use of the MesoWest website and do not query it too frequently.

Any errors that occurred during retrieval or parsing of the observations can be examined with

    raws_ingest:report_errors()

and cleared with

    raws_ingest:clear_errors()


# Caveats

  * if a station does not have a sensor with a variable, the MesoWest website will return nothing, thr library currently does nothing about this

