"""
This files contains the SQL queries used in this project
"""

# Creation of an empty fact table
sql_empty_trips = """
    CREATE TABLE IF NOT EXISTS public.trips (
        trip_id       int        IDENTITY PRIMARY KEY,
        station_start int        NOT NULL, 
        station_end   int        NOT NULL,
        date_start    timestamp  NOT NULL,
        date_end      timestamp  NOT NULL,
        weather_id    timestamp,
        trip_duration int,
        is_member     BOOL )
    """


sql_empty_stations = """
    CREATE TABLE IF NOT EXISTS public.stations (
        station_id   int          PRIMARY KEY,
        latitude     real         NOT NULL, 
        longitude    real         NOT NULL,
        name         varchar(256) NOT NULL )
    """

sql_empty_weather = """
    CREATE TABLE IF NOT EXISTS public.daily_weather (
        weather_id     timestamp  PRIMARY KEY,
        temperature    real,
        precipitation  real )
    """