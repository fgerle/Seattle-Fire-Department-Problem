# The Seattle Fire Department Problem

We want to predict the number of emergency calls in Seattle for a given date. This project consists of two main parts. 

1) The callDB module
2) A Jupyter Notebook

## The callDB module

The callDB module is used to read the data of 911 calls obtained from https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj into a sqlite3 database and build daily statistics. The data can be extended by weather data and data on COVID19 infection numbers.

### Parameters
- `csvfile=None`: name of the csv file containing emergency call data. Be sure to download a
          csv-file from https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj.

- `db_file='calls.db'`: name of the database file.

- `weather_data=None`: Name of a csv file containing weather information.

- `covid_data=None`: Name of a csv file conatinaing information on COVID numbers for Seattle.

- `rebuild=False`: if True the tables will we dropped from the database and then rebuilt. Otherways,
          already existing entries will be skipped and not updated.

- `load_only=False`: when true, an existing database file will be loaded and no new data is read.
          In that case the csfile can be omitted.

### Attributes
- `population`: historical population data for Seattle. Source:  https://www.macrotrends.net/cities/23140/seattle/population
- `weather_cond_dict`: Translation of weather condidition codes. For further details see https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt.
- `weather_info`: boolean flag to indicate the existence of weather info in the database.
- `covid_info`: boolean flag to indicate the existence of covid info in the database.
- `wa_holidays`: public holidays for Washington state (type: holidays).
- `holiday_dict`: holds the names to integer encoded holidays.
- `first_timestamp`: first timestamp of data.
- `last_timestamp`: last timestamp of data.
- `first_date`: first date with data.
- `last_date`: last date with data.

### Methods
- `get_season(date)`: Returns the season of a specified date. This method uses meteological rather than
        astronomical seasons. The returned value is dictionary of boolean values (0/1)
        indicating the sewwwason.

- `exists(ID, table="calls", connection=None, cursor=None)`: Check if an entry with a given id already exists.
              
- `load_covid_info(filename, rebuild=False, connection=None, cursor=None)`: Load COVID pandemic details from file `filename`.  Set the `rebuild` to `True` if an existing database should be overwritten

- `get_covid_info(date, connection=None, cursor=None)`: Get the Covid details for date from the database. Returns a dictionary if an entry
        exists and None otherways.

- `load_weather_info(filename, data_filter=None, rebuild=False, connection=None, cursor=None)`: Load the weather info from a .csv file. A `data_filter` can be specified to include only 
        filtered columns in the database.        

- `query_db(query, data=(), connection=None, cursor=None)`: Perform a SQL query on the database and return the results

- `write_db(query, data=(), commit = False, connection=None, cursor=None)`: Perform a SQL command on the database

- `get_type_stats(date=None, start_ts=None, end_ts=None)`: Returns a statistic of the 911 calls between two timestamps. I.e. the number of calls for the different types. Returns a dictionary.

- `get_date_details(date)`: Retrieve the details for a given date from the database.

- `get_weather(date, connection=None, cursor=None)`: Get weather details for date. Returns an empty dictionary if no entry exists.

- `get_hourly_stats(date=None, start_ts=None, end_ts=None)`: Returns a hourly statistic of the 911 calls between two timestamps. I.e. the number of calls at different hours of the day. Returns a tuple.

- `count_daily(date=None, date_tuple=None, timestamp=None)`: Count the number of 911 calls on a given date. Dates can be provided as strings of the
        form 'YYYY-MM-DD', as tuples of the form (YYYY, MM, DD) or as a timestamp.
        If the date is provided as a timestamp, the result will be the number of calls
        on the day to which the timestamp belongs.

- `count_between(start=None, end=None, start_ts=None, end_ts=None)`: Count the number of 911 calls between two timepoints. Times must be given either as a string of the form 'YYYY-MM-DD HH:MM:SS' or as a timestamp.

## Jupyter notebook 911_calls.ipynb

Provides a short analysis of the data and a comparison of different learning strategies. Via writefile the important parts are written to train.py which can be used to get predictions for user specified dates
