from call_db.calls import callDB
from pop_predict.predict import populationPredictor
import numpy as np
import math
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.feature_selection import mutual_info_classif
from sklearn.feature_selection import f_classif
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
import sklearn.metrics as metrics
import datetime as dt

def collect_data(call_db, first_date, last_date, categories=None):
    data = []

    delta = dt.timedelta(days=1)
    day = first_date

    while day <= last_date:
        details = call_db.get_date_details(day)
        weather = call_db.get_weather(day)
        covid = call_db.get_covid_info(day)
        weekend = 0
        holiday = 0
        snow = 0
    
        if details["weekday"] >= 5:
            weekend = 1
        if details["holiday"] > 0:
            holiday = 1
        if weather["SNOW"] > 0:
            snow = 1
            
        daily_data = {
            #"HOURLY_CALLS": details["hourly_stats"],
            "CALLS": details["calls"],
            "DAY": details["day"],
            "MONTH": details["month"],
            "YEAR": details["year"],        
            "HOLIDAY": details["holiday"],
            "WEEKEND_BOOL": weekend,
            "HOLIDAY_BOOL": holiday,
            "WEEKDAY": details["weekday"],
            "POP": details["population"],
            "DAY_YEAR": details["day_of_year"],
            "WINTER": details["Winter"],
            "SPRING": details["Spring"],
            "SUMMER": details["Summer"], 
            "FALL": details["Fall"],
            "TMIN": weather["TMIN"],
            "TMAX": weather["TMAX"],
            "TAVG": weather["TAVG"],
            "SNOW": weather["SNOW"],
            "SNOW_BOOL": snow,
            "PRCP": weather["PRCP"],
            "FOG": weather["WT01"],
            "HVY_FOG": weather["WT02"],
            "THUNDER": weather["WT03"],
            "ICE": weather["WT04"],
            "HAIL": weather["WT05"],
            "GLAZE": weather["WT06"],
            "HAZE": weather["WT08"],    
            "PCR_TESTS": covid["pcr_test"],
            "WEEKLY_PCR_TESTS": covid["seven_day_pcr_test"],
            "PCR_TESTS_POS": covid["pcr_pos"],
            "WEEKLY_PCR_TESTS_POS": covid["seven_day_pcr_pos"],
            "HOSP_CNT": covid["hosp_cnt"],
            "DEATH_CNT": covid["death_cnt"],
            "WEEKLY_HOSP_CNT": covid["seven_day_hosp_cnt"],
            "WEEKLY_DEATH_CNT": covid["seven_day_death_cnt"],
            "PANDEMIC": covid["pandemic"]
        }

        if categories != None:
            sum = 0
            cat_data = details["type_stats"]
            for cat in categories:
                if cat not in cat_data.keys():
                    daily_data.update({cat: 0})
                else:
                    daily_data.update({cat: cat_data[cat]})
                    sum += cat_data[cat]
            daily_data.update({"Misc Emergencies": (details["calls"] - sum)})
   
        data.append(daily_data)
        day += delta

    return(pd.DataFrame(data))

def calls_estimator(relevant_features):
    full_data = collect_data(db, db.first_date, db.last_date)
    rand_training_X, rand_test_X, rand_training_y, rand_test_y = train_test_split(full_data[relevant_features], full_data["CALLS"], random_state=42, test_size=0.25)
    model = GradientBoostingRegressor(n_estimators=250,
                                    learning_rate = 0.2,
                                    subsample = 0.75).fit(rand_training_X, rand_training_y)
    return(model)

if __name__ == "__main__":
    db = callDB("Seattle_Real_Time_Fire_911_Calls_20240111.csv", weather_data="weather_data.csv", covid_data= "COVID.csv", rebuild=True, dmin="2010-01-01", load_only=True)
    relevant_features = ["POP", "PRCP", "MONTH", "TMAX", "SNOW_BOOL", "SUMMER", "WINTER", "HAZE", "HAIL", "WEEKEND_BOOL", "HOLIDAY_BOOL", "WEEKLY_HOSP_CNT", "WEEKLY_DEATH_CNT", "WEEKLY_PCR_TESTS", "WEEKLY_PCR_TESTS_POS", "PANDEMIC"]
    model = calls_estimator(relevant_features)
    date_str = input("Please enter the date for which the 911-Calls should be predicted. Use the format YYYY-MM-DD.")
    weather = input(f"Do you have weather information for {date_str}? y/N")
    if weather in ["y","yes", "Y", "YES", "Yes"]:
        PRCP = input(f"WEATHER: Enter the amount of precipitation for {date_str}. Press ENTER to skip.")
        if PRCP == "":
            PRCP = 0
        TMAX = input(f"WEATHER: Enter the maximal temperature (in degrees Celsius) for {date_str}. Press ENTER to skip.")
        if TMAX == "":
            TMAX = 18
        SNOW_BOOL = input(f"WEATHER: Is it going to snow on {date_str}? Enter 0 for No and 1 for Yes. Press ENTER to skip.")
        if SNOW_BOOL == "":
            SNOW_BOOL = 0
        HAZE = input(f"WEATHER: Is it going to be hazy on {date_str}? Enter 0 for No and 1 for Yes. Press ENTER to skip.")
        if HAZE == "":
            HAZE = 0        
        HAIL = input(f"WEATHER: Is hail expected on {date_str}? Enter 0 for No and 1 for Yes. Press ENTER to skip.")
        if HAIL == "":
            HAIL = 0
    else:
        PRCP = 0
        TMAX = 18
        SNOW_BOOL = 0
        HAZE = 0
        HAIL = 0
    covid = input(f"Do you have covid information for {date_str}? y/N")
    if covid in ["y","yes", "Y", "YES", "Yes"]:
        WEEKLY_HOSP_CNT = input("How many people were hospitalized within the past seven days with COVID? Press ENTER to skip.")
        if WEEKLY_HOSP_CNT == "":
            WEEKLY_HOSP_CNT = 0
        WEEKLY_DEATH_CNT = input("How many people died within the past seven days from COVID? Press ENTER to skip.")
        if WEEKLY_DEATH_CNT == "":
            WEEKLY_DEATH_CNT = 0
        WEEKLY_PCR_TESTS = input("How many PCR tests were registered within the last 7 days? Press ENTER to skip.")
        if WEEKLY_PCR_TESTS == "":
            WEEKLY_PCR_TESTS = 0
        WEEKLY_PCR_TESTS_POS = input("How many positive PCR tests were registered within the last 7 days? Press ENTER to skip.")
        if WEEKLY_PCR_TESTS_POS == "":
            WEEKLY_PCR_TESTS_POS = 0
    else:
        WEEKLY_HOSP_CNT = 0
        WEEKLY_DEATH_CNT = 0
        WEEKLY_PCR_TESTS = 0
        WEEKLY_PCR_TESTS_POS = 0

    pop = populationPredictor()

    date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()

    weekend = 0
    if date.weekday() in [5,6]:
        weekend = 1

    holiday = 0
    if date in db.wa_holidays:
        holiday = 1

    pandemic_start = dt.date(2020,1,30)
    pandemic_end = dt.date(2023,5,5)

    pandemic = 0
    
    if date > pandemic_start and date < pandemic_end:
        pandemic = 1
    

    details = [{"POP": pop.predict(date.year),
               "PRCP": PRCP,
               "MONTH": date.month,
               "TMAX": TMAX,
               "SNOW_BOOL": SNOW_BOOL,
               "SUMMER": db.get_season(date)["Summer"],
               "WINTER": db.get_season(date)["Winter"],
               "HAZE": HAZE,
               "HAIL": HAIL,
               "WEEKEND_BOOL": weekend,
               "HOLIDAY_BOOL": holiday,
               "WEEKLY_HOSP_CNT": WEEKLY_HOSP_CNT,
               "WEEKLY_DEATH_CNT": WEEKLY_DEATH_CNT,
               "WEEKLY_PCR_TESTS": WEEKLY_PCR_TESTS,
               "WEEKLY_PCR_TESTS_POS": WEEKLY_PCR_TESTS_POS,
               "PANDEMIC": pandemic},]

    print(details)
    calls_prediction = model.predict(pd.DataFrame(details))

    print(f"For {date_str} a total of {calls_prediction} Emergency calls is predicted. The standard deviation of this estimator is less than 25 calls.")
               
               
    
