import pytest

from call_db.calls import callDB
import datetime as dt

start_date = "2022-01-01"
end_date = "2022-01-31"
weather_fn = "weather_data.csv"
csv_fn = "test.csv"#Seattle_Real_Time_Fire_911_Calls_20240111.csv"

db_fn = "test.db"

#call(["rm", db_fn])
db = callDB(csv_fn, db_fn, rebuild=True, dmin=start_date, dmax=end_date)

def test_init():
    assert db.db_file == db_fn

def test_query_db():    
    id = "F220009061"

    result = db.query_db('SELECT type FROM calls WHERE id = ?;', (id,))
    assert result == [('Rescue Elevator',)]

def test_count_daily():
    year = 2022
    month = 1
    day = 6

    result = db.count_daily(date_tuple=(year, month, day))
    assert result == 336

    result = db.count_daily(date=f"{year}-{month}-{day}")
    assert result == 336

    result = db.count_daily(timestamp=dt.datetime(year=year, month=month,
                             day=day, hour=13, minute=12, second=45).timestamp())
    assert result == 336

    
def test_count_between():

    result = db.count_between("2020")
    assert result == 0

    result = db.count_between()
    assert result == 9588

    result = db.count_between("2022-01-01 00:00:00", "2022-01-7 23:59:59")
    assert result == 2511

def test_get_type_stats():

    result = db.get_type_stats(date="2022-01-13")
    assert result == {'Aid Response': 133,
         'Medic Response': 35,
         'Trans to AMR': 32,
         'Auto Fire Alarm': 20,
         'Low Acuity Response': 16,
         'Triaged Incident': 14,
         'Illegal Burn': 6,
         'Rescue Elevator': 6,
         'MVI - Motor Vehicle Incident': 6,
         'Rubbish Fire': 6,
         'Medic Response- 7 per Rule': 5,
         '1RED 1 Unit': 5,
         'Alarm Bell': 5,
         'Encampment Fire': 4,
         'Automatic Fire Alarm Resd': 4,
         'Activated CO Detector': 2,
         'Wires Down': 2,
         '4RED - 2 + 1 + 1': 2,
         'AFA4 - Auto Alarm 2 + 1 + 1': 2,
         'Natural Gas Odor': 2,
         'Medic Response- 6 per Rule': 2,
         'Investigate Out Of Service': 2,
         'Aid Response Yellow': 2,
         'Referral To Agency': 1,
         'Rescue Extrication': 1,
         'Rescue Lock In/Out': 1,
         'Brush Fire': 1,
         'Automatic Fire Alarm False': 1}
    

def test_get_time_stats():    

    result = db.get_hourly_stats(date="2022-01-13")
    assert result == [17, 11, 4, 7, 5, 11, 12, 12, 12, 16, 11, 17, 15, 20, 17, 14, 12, 20, 19, 16, 18, 13, 13, 6]


def test_get_date_details():
    result = db.get_date_details(date="2022-01-13")
    assert result == {'date': '2022-01-13',
 'year': 2022,
 'month': 1,
 'day': 13,
 'day_of_year': 13,
 'population': 3489000,
 'calls': 318,
 'type_stats': {'Activated CO Detector': 2,
  'Trans to AMR': 32,
  'Triaged Incident': 14,
  'Medic Response': 35,
  'Auto Fire Alarm': 20,
  'Wires Down': 2,
  'Aid Response': 133,
  'Low Acuity Response': 16,
  'Medic Response- 7 per Rule': 5,
  '4RED - 2 + 1 + 1': 2,
  'Illegal Burn': 6,
  'Referral To Agency': 1,
  'AFA4 - Auto Alarm 2 + 1 + 1': 2,
  'Natural Gas Odor': 2,
  'Rescue Elevator': 6,
  'Medic Response- 6 per Rule': 2,
  '1RED 1 Unit': 5,
  'Alarm Bell': 5,
  'Encampment Fire': 4,
  'Investigate Out Of Service': 2,
  'Rescue Extrication': 1,
  'MVI - Motor Vehicle Incident': 6,
  'Automatic Fire Alarm Resd': 4,
  'Rubbish Fire': 6,
  'Rescue Lock In/Out': 1,
  'Aid Response Yellow': 2,
  'Brush Fire': 1,
  'Automatic Fire Alarm False': 1},
 'weekday': 3,
 'holiday': 0,
 'holiday_name': None,
 'season_name': 'Winter',
 'Winter': 1,
 'Spring': 0,
 'Summer': 0,
 'Fall': 0}

def test_load_weather_info():
    db.load_weather_info("weather_data.csv", rebuild=True)
    assert db.get_weather("2022-01-13")["TMIN"] == 3.9

    result = db.get_weather("2022-01-16")
    assert result == {'STATION': 'USW00094290',
 'NAME': 'SEATTLE SAND POINT WEATHER FORECAST OFFICE, WA US',
 'DATE': '2022-01-16',
 'AWND': 0.0,
 'PRCP': 0.0,
 'SNOW': 0.0,
 'SNWD': 0.0,
 'TAVG': 1.1,
 'TMAX': 6.1,
 'TMIN': 3.9,
 'WDF2': 0.0,
 'WDF5': 0.0,
 'WSF2': 0.0,
 'WSF5': 0.0,
 'WT01': 1.0,
 'WT02': 0.0,
 'WT03': 0.0,
 'WT04': 0.0,
 'WT05': 0.0,
 'WT06': 0.0,
 'WT08': 0.0}
