import sqlite3
import csv
import datetime as dt
import time
from collections import Counter
import holidays
import json



class callDB:
    """
    The callDB class is used to aquire data of 911 call in Seattle
    from the web, stores the data in a sqlite3 database and provides
    ready to use methods for fetching data.
    """

    
    def __init__(self, csvfile=None, db_file='calls.db', weather_data=None, covid_data=None, rebuild=False, load_only=False, dmin=None, dmax=None):
        """
        Create the database and fetch data from source csv file.
        - csvfile=None: name of the csv file containing emergency call data. Be sure to download a
          csv-file from https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj.
        - db_file='calls.db': name of the database file.
        - weather_data=None: Name of a csv file containing weather information.
        - covid_data=None: Name of a csv file conatinaing information on COVID numbers for Seattle.
        - rebuild=False: if True the tables will we dropped from the database and then rebuilt. Otherways,
          already existing entries will be skipped and not updated.
        - load_only=False: when true, an existing database file will be loaded and no new data is read.
          In that case the csfile can be omitted.
        """

        # Load historical population data for Seattle. Source:
        # https://www.macrotrends.net/cities/23140/seattle/population
        self.population = {2024: 3549000, 2023: 3519000, 2022: 3489000, 2021: 3461000, 2020: 3433000,
                        2019: 3406000, 2018: 3379000, 2017: 3339000, 2016: 3299000, 2015: 3259000,
                        2014: 3220000, 2013: 3182000, 2012: 3143000, 2011: 3106000, 2010: 3069000,
                        2009: 3032000, 2008: 2996000, 2007: 2960000, 2006: 2924000, 2005: 2889000,
                        2004: 2855000, 2003: 2820000, 2002: 2787000, 2001: 2753000, 2000: 2720000,
                        1999: 2669000, 1998: 2613000, 1997: 2559000, 1996: 2505000, 1995: 2453000,
                        1994: 2401000, 1993: 2351000, 1992: 2302000, 1991: 2253000, 1990: 2206000,
                        1989: 2160000, 1988: 2114000, 1987: 2069000, 1986: 2025000, 1985: 1982000,
                        1984: 1940000, 1983: 1899000, 1982: 1858000, 1981: 1819000, 1980: 1780000,
                        1979: 1753000, 1978: 1730000, 1977: 1707000, 1976: 1685000, 1975: 1663000,
                        1974: 1641000, 1973: 1619000, 1972: 1598000, 1971: 1577000, 1970: 1556000,
                        1969: 1509000, 1968: 1456000, 1967: 1404000, 1966: 1354000, 1965: 1305000,
                        1964: 1259000, 1963: 1214000, 1962: 1171000, 1961: 1129000, 1960: 1089000,
                        1959: 1054000, 1958: 1021000}

        # Translation of weather condidition codes. For further details see https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt.
        self.weather_cond_dict = {"WT01" : "Fog, ice fog, or freezing fog (may include heavy fog)",
                "WT02" : "Heavy fog or heaving freezing fog (not always distinquished from fog)",
                "WT03" : "Thunder",
                "WT04" : "Ice pellets, sleet, snow pellets, or small hail",
                "WT05" : "Hail (may include small hail)",
                "WT06" : "Glaze or rime",
                "WT07" : "Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction",
                "WT08" : "Smoke or haze",
                "WT09" : "Blowing or drifting snow",
                "WT10" : "Tornado, waterspout, or funnel cloud",
                "WT11" : "High or damaging winds",
                "WT12" : "Blowing spray",
                "WT13" : "Mist",
                "WT14" : "Drizzle",
                "WT15" : "Freezing drizzle",
                "WT16" : "Rain (may include freezing rain, drizzle, and freezing drizzle)", 
                "WT17" : "Freezing rain", 
                "WT18" : "Snow, snow pellets, snow grains, or ice crystals",
                "WT19" : "Unknown source of precipitation",
                "WT21" : "Ground fog",
                "WT22" : "Ice fog or freezing fog"}

        # Set the database version to make sure that we use a compatible database if the 
        # load_only flag is set to True
        self.__version = "v0.3.0"
        
        # Boolean variables to indicate if the database contains weather/covid information.
        # Set to False a priori.
        self.weather_info = False
        self.covid_info = False

        self.db_file = db_file
        
        # Create a holiday object to determine if a given date is a holiday.
        self.wa_holidays = holidays.country_holidays('US', subdiv="WA")

        # Initialize the holiday_dict dictionary. We will encode the holdidays by an integer 
        # and this dictionary is used to translate this integer back to the name of the holiday.
        self.holiday_dict = {"None": 0}

        # Check if the load_only flag is set to true.
        if load_only:
            
            # First validate thhe database version.
            if not self.__verify_version():
                raise ValueError("Wrong database version. Clear database or rebuild!")

            # Build the holiday dictionary.
            self.__build_holiday_dict()

            # Check if the database contains weather data.
            if "weather" in [col[1] for col in self.query_db("PRAGMA table_info(dates)")]:
                self.weather_info = True

            # Check if the database contains covid data.
            if "covid" in "covid" in [col[1] for col in self.query_db("PRAGMA table_list")]:
                self.covid_info = True

            # Set timestamp and date of first and last day in database.
            self.first_timestamp = self.query_db("SELECT MIN(timestamp) FROM calls;")[0][0]
            self.first_date = dt.date.fromtimestamp(self.first_timestamp)

            self.last_timestamp = self.query_db("SELECT MAX(timestamp) FROM calls;")[0][0]
            self.last_date = dt.date.fromtimestamp(self.last_timestamp)

            # End here if load_only is true.
            return
            
        # Initiate the database
        self.__init_db(rebuild=rebuild)

        # Parse the csv file containing the 911-calls and write to db.
        print("Parsing input file...")
        call_data = self.__parse_csv(csvfile, dmin=dmin, dmax=dmax)
        self.__write_to_db("calls", call_data)
        print("Done.")

        # Set timestamp and date of first and last day in database.
        self.first_timestamp = self.query_db("SELECT MIN(timestamp) FROM calls;")[0][0]
        self.first_date = dt.date.fromtimestamp(self.first_timestamp)

        self.last_timestamp = self.query_db("SELECT MAX(timestamp) FROM calls;")[0][0]
        self.last_date = dt.date.fromtimestamp(self.last_timestamp)

        # Create the table with daily infos
        print("Collecting daily info...")
        self.__create_dates_table()
        print("Done.")

        # Load weather data if specified.
        if weather_data != None:
            print("Loading weather info...")
            self.load_weather_info(weather_data, rebuild=rebuild)
            print("Done.")

        # Load covid data if specified.
        if covid_data != None:
            print("Loading covid info...")
            self.load_covid_info(covid_data, rebuild=rebuild)
            print("Done.")

    def __parse_csv(self, csvfile, dmin=None, dmax=None):
        """
        Parse a csv-file containing data and 911-calls. Data can be retrieved from
        https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj

        The output is a list containing tuples which can be written to the database.
        """

        # Initialize variables to count holidays (hd_cnt) and for the output data (data)
        hd_cnt = 1
        data = []

        # Open the csv file and iterate over the rows of the file.
        with open(csvfile, 'r') as f:
            linereader = csv.reader(f)
            firstline = True
            for row in linereader:                   
                # Initialize holiday variable to 0 which represents None or no holiday.
                hd = 0
                hd_name = None
                
                # The first line contains column names. Treat differently and verify that
                # the file has the correct format.
                if firstline:
                    if row != ['Address', 'Type', 'Datetime', 'Latitude', 'Longitude', 'Report Location', 'Incident Number']:
                        print("""
The csv file seems to be malformed. 
Did you download the correct file from https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj ?
                        """)
                        conn.close()
                        raise(ValueError)         
                           
                    firstline = False
                else:
                    
                    # Convert time.
                    t = dt.datetime.strptime(row[2], "%m/%d/%Y %I:%M:%S %p")
                    ts = int(t.timestamp())

                    # Check if date is within boundaries.
                    if dmin != None:
                        date_min = dt.datetime.strptime(dmin,"%Y-%m-%d")
                        if t < date_min:
                            continue
                    if dmax != None:
                        date_max = dt.datetime.strptime(dmax,"%Y-%m-%d")
                        if t > date_max:
                            continue

                    # Try to convert coordinates to float.
                    try:
                        lat = float(row[3])
                        lon = float(row[4])
                    except ValueError:
                        lat,lon=None,None

                    # Check if the date is a holiday and update the holiday_dict accordingly
                    if t in self.wa_holidays:
                        hd_name = self.wa_holidays.get(t)
                        if hd_name in self.holiday_dict.keys():
                            hd = self.holiday_dict[hd_name]
                        else:
                            self.holiday_dict.update({hd_name: hd_cnt})
                            hd = hd_cnt
                            hd_cnt += 1                            

                    # Append tuple to data-list.
                    data.append((row[6], row[0], t.strftime("%Y-%m-%d"), row[1], t.year,
                                 t.month, t.day, t.weekday(), t.hour, t.minute, t.second,
                                 hd_name, ts, lat, lon))
        return(data)

    def __write_to_db(self, table, data, connection=None, cursor=None):
        """
        Write data to database using the executemany method. The name of the table to insert the data
        and the data must be given. This method uses "INSERT OR IGNORE" so existing entries will not 
        be updated.
        """
        # Get cursor and connection. Create if neccessary.
        cur,conn = self.__get_cursor(connection, cursor)

        # Count the number of columns of the given table.
        col_query = f"PRAGMA TABLE_INFO({table});"
        cols = len(self.query_db(col_query, connection=conn, cursor=cur))
        
        # Build the SQL query.
        data_placeholder = "(" + ",".join(["?" for i in range(cols)]) + ")"
        sql_command = f"INSERT OR IGNORE INTO {table} VALUES {data_placeholder};"

        # Execute command and commit changes to database.
        cur.executemany(sql_command, data)
        conn.commit()

        # Close connection if newly created.
        if connection==None:
            conn.close
        

            
    def get_season(self, date):
        """
        Returns the season of a specified date. This method uses meteological rather than
        astronomical seasons. The returned value is dictionary of boolean values (0/1)
        indicating the season.
        """
        year = date.year
        winter, spring, summer, fall = 0,0,0,0
        if date < dt.date(year = year, month=3, day=1):
            winter = 1
        elif date < dt.date(year=year, month=6, day=1):
            spring = 1
        elif date < dt.date(year=year, month=9, day=1):
            summer = 1
        elif date < dt.date(year=year, month=12, day=1):
            fall = 1
        else:
            winter = 1
        return({"Winter":winter, "Spring":spring, "Summer":summer, "Fall": fall})
        
       
    def __create_connection(self):
        """ 
        Create a database connection to a SQLite database 
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            #print(sqlite3.version)
        except Error as e:
            print(e)
        finally:
            if conn:
                return(conn)

    def __verify_version(self, connection=None, cursor=None):
        """
        Verify the database version
        """
        cur,conn = self.__get_cursor(connection, cursor)
        db_version = None
        try:
            result = self.query_db("SELECT * FROM version;", connection=conn, cursor=cur)
        except OperationalError:
            return(False)
        if [(self.__version,)] == result:
            return(True)
        else:
            return(False)
            
        
        
            
    def __get_cursor(self, connection=None, cursor=None):
        """
        Create a database connection and cursor for database
        """
        if connection == None:
            conn = self.__create_connection()
            cur = conn.cursor()
        elif cursor == None:
            conn = connection
            cur = conn.cursor()
        else:
            conn = connection
            cur = cursor
        return(cur, conn)

    def __build_holiday_dict(self, connection=None, cursor=None):
        """
        Build a dictionary of holiday names with their associated numbers
        """

        cur,conn = self.__get_cursor()
        holidays = set(self.query_db("SELECT holiday FROM dates;", connection=conn, cursor=cur))

        hd_cnt = 1
        
        for hd in holidays:
            if hd[0] != None:
                self.holiday_dict.update({hd[0]: hd_cnt})
                hd_cnt += 1
                

    def __create_dates_table(self, connection=None, cursor=None):
        """
        Create the table containing daily info.
        """
        cur,conn = self.__get_cursor(connection, cursor)

        # Build SQL command.
        create_dates_table = """CREATE TABLE IF NOT EXISTS dates (
                                id text PRIMARY KEY,
                                year int,
                                month int,
                                day int,
                                day_of_year int,
                                weekday int,
                                holiday text,
                                calls int,        
                                details text,
                                population int
                              );"""

        # Execute cmannd and create table.
        cur.execute(create_dates_table)

        # Collect daily data.
        dates_data = self.__collect_dates_info(self.first_date, self.last_date)

        # And write data to db.
        self.__write_to_db("dates", dates_data)

        if connection == None:
            conn.close()


    def __collect_dates_info(self, first_day, last_day):
        """
        Collect daily information for all days between first_day and last_day. 
        Returns a list of tuples.
        """

        # Set variables for looping through all dates within bounds.
        delta = dt.timedelta(days=1)
        day = first_day
        total_days = abs(last_day - first_day).days + 1
        day_cnt=0

        # Set number of holidays in dictionary.
        hd_cnt = max(self.holiday_dict.values())+1

        # Initialize return data list.
        data = []
        
        # Loop over days.
        while day <= last_day:

            # Output progress every 10 processed days.
            if day_cnt%10 == 0:
                print(f"Collecting daily data for day {day_cnt}/{total_days}", end="\r")

            # Count loop repetitions
            day_cnt += 1
            
            # Initialize holiday variable to 0 or "no holiday".
            hd = 0

            # convert day to string.
            date = day.strftime("%Y-%m-%d")

            # Calculate number of the day as day of the year.
            day_of_year = abs(day - dt.date(year=day.year, month=1, day=1)).days + 1

            # Get the number of the day within the week, 0=Monday.
            weekday = day.weekday()

            # Check if the day is a holiday.
            if day in self.wa_holidays:

                # Save the name of the holiday.
                holiday = self.wa_holidays.get(day)

                # Get the number of the holiday from the holiday_dict
                if holiday in self.holiday_dict:
                    hd = self.holiday_dict[holiday]

                # Update the holiday_dict if holiday is not yet in the dictionary
                else:
                    self.holiday_dict.update({holiday: hd_cnt})
                    hd = hd_cnt
                    hd_cnt += 1
            
            # If not a holiday, set holiday to None.
            else:
                holiday = None

            # Count the number of calls on the day.               
            calls = self.count_daily(date)

            # Get the population data for that year.
            pop = self.population[day.year]

            # Get numbers of calls per type of emergency.
            type_stats = self.get_type_stats(date)

            # Get number of calls per hour.
            hourly_stats = self.get_hourly_stats(date)

            # Get the season.
            season = self.get_season(day)

            # Translate season dictionary to season name.
            season_name = list(season.keys())[list(season.values()).index(1)]

            # Create details dictionary.
            details = {"date": date, "year": day.year, "month": day.month,
                       "day": day.day, "day_of_year": day_of_year, "population":pop, "calls": calls, "type_stats": type_stats,
                       "weekday": weekday, "holiday": hd, "holiday_name": holiday, "season_name": season_name, "hourly_stats":hourly_stats}

            # Update the details dictionary with the season dictionary.
            details.update(season)

            # Dump the whole dictionary to a json formatted string.
            details_txt = json.dumps(details)
            
            # Append info to data list.
            data.append((date, day.year, day_of_year, day.month, day.day, weekday, holiday, calls, details_txt, pop))
            
            day += delta
        
        print(f"Collecting daily data for day {total_days}/{total_days}")

        return(data)
        

    def __insert_entry(self, entry, connection=None, cursor=None):
        """
        Insert an entry into the database without checking if it already exists.
        """
        cur,conn = self.__get_cursor(connection, cursor)        
        cur.execute("INSERT INTO calls VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", entry)
        if connection == None:
            conn.close()


    def __create_column(self, table, column, col_type, rebuild=False, connection=None, cursor=None):
        """
        Create a new column in the table. Drop column first, if rebuild flag is True.
        """
        cur,conn = self.__get_cursor(connection, cursor)

        table_info = self.query_db(f"PRAGMA table_info({table})")

        cols = [row[1] for row in table_info]

        if column in cols:
            if rebuild:
                self.query_db(f"ALTER TABLE {table} DROP COLUMN {column};", connection=conn, cursor=cur)
                self.query_db(f"ALTER TABLE {table} ADD COLUMN {column} {col_type};", connection=conn, cursor=cur)
        else:
            self.query_db(f"ALTER TABLE {table} ADD COLUMN {column} text;", connection=conn, cursor=cur)

        if connection == None:
            conn.close()
            
               
    def __init_db(self, connection=None, cursor=None, rebuild=False):
        """ Initialize sqlite database and create main table """
        cur,conn = self.__get_cursor(connection,cursor)
        
        sql_create_main_table = """CREATE TABLE IF NOT EXISTS calls (
                                id text PRIMARY KEY,
                                date text,
                                address text,
                                type text,
                                year int,
                                month int,
                                day int,
                                week_day int,
                                hour int,
                                minute int,
                                second int,
                                holiday text,
                                timestamp int,
                                lat real,
                                lon real
                              );"""
        if conn is not None:
            try:
                if rebuild:
                    cur.execute("DROP TABLE IF EXISTS calls;")
                    cur.execute("DROP TABLE IF EXISTS dates;")                
                cur.execute(sql_create_main_table)
                cur.execute("DROP TABLE IF EXISTS version;")
                cur.execute("CREATE TABLE version (id text PRIMARY KEY);")
                cur.execute("INSERT INTO version VALUES (?);", (self.__version,))
                conn.commit()
            except:
                pass
        else:
            print("Cannot create database connection")
        if connection == None:            
            conn.close()
            

    def __date_to_ts(self, date):
        """
        Convert a date (either as string or as date object) to a pair of timestamps
        indicating the beginning and the end of that day.
        """

        # Convert date to datetime object
        if type(date) == str:                   
            day = dt.datetime.strptime(date, "%Y-%m-%d").date()
        elif type(date) == dt.datetime:
            day = date.date()
        elif type(date) == dt.date:
            day = date

        start_ts = dt.datetime(day.year, day.month, day.day, 0,0,0).timestamp()
        end_ts = dt.datetime(day.year, day.month, day.day, 23,59,59).timestamp()
        return(start_ts,end_ts)


    def exists(self, ID, table="calls", connection=None, cursor=None):
        """
        Check if an entry with a given id already exists.
        """
        
        cur,conn = self.__get_cursor(connection, cursor)
        query = f"SELECT EXISTS(SELECT * FROM {table} WHERE id = ?);"
        cur.execute(query, (ID,))
        p = cur.fetchall()[0][0]        
        if connection == None:
            conn.close()
        if p == 1:
            return(True)
        else:
            return(False)

    def load_covid_info(self, filename, rebuild=False, connection=None, cursor=None):
        """
        Load COVID pandemic details from file. 
        """
        
        # Initialize data list
        data = []

        # Get database connection and cursor
        cur,conn = self.__get_cursor(connection, cursor)

        # Set the beginning and the end of the COVID pandemic. 
        # The World Health Organization (WHO) declared the outbreak
        # a public health emergency of international concern (PHEIC)
        # on 30 January 2020. The WHO ended its PHEIC declaration on 5 May 2023.
        pandemic_start = dt.date(2020,1,30)
        pandemic_end = dt.date(2023,5,5)

        if rebuild:
            cur.execute("DROP TABLE IF EXISTS covid")        

        # SQL command to create new covid table            
        create_covid_table = """CREATE TABLE IF NOT EXISTS covid (
                                id text PRIMARY KEY,
                                pandemic int,
                                pcr_test int,
                                pcr_pos int,
                                hosp_cnt int,
                                death_cnt int,                            
                                seven_day_pcr_test int,
                                seven_day_pcr_pos int,
                                seven_day_hosp_cnt int,
                                seven_day_death_cnt int
                                );"""
        
        # Create new table.
        cur.execute(create_covid_table)

        # Parse the csv file.
        with open(filename, 'r') as f:
            linereader = csv.reader(f)
            firstline = True

            # Iterate over the rows of file.
            for row in linereader:  

                # Treat the first line differntly.
                if firstline:
                    firstline=False
                    continue

                # Initialize variables
                pcr_pos,pcr_test,death_cnt,hosp_cnt = 0,0,0,0

                # Get the date from the row
                date = dt.datetime.strptime(row[2], "%m.%d.%Y").date()    

                # Check if the date falls within the pandemic and set pandemic variable accordingly.
                if date < pandemic_end and date > pandemic_start:
                    pandemic = 1
                else:
                    pandemic = 0

                # Convert date to string.
                date_str = date.strftime("%Y-%m-%d")

                # Only add an entry if the date exists in the table of dates.           
                if self.exists(date, table="dates"):

                    # If the entry does not yet exist or the rebuild flag is set, create an entry.
                    if rebuild or self.get_covid_info(date) == None:
                        try:
                            pcr_test = int(row[6])
                        except ValueError:
                            pcr_test = 0
                        try:
                            pcr_pos = int(row[7])
                        except ValueError:
                            pcr_pos = 0
                        try:
                            death_cnt = int(row[5])
                        except ValueError:
                            death_cnt = 0
                        try:
                            hosp_cnt = int(row[4])
                        except ValueError:
                            hosp_cnt = 0
                        data.append((date_str,pandemic, pcr_test, pcr_pos, hosp_cnt, death_cnt,0,0,0,0))
        
        # Write all new entries to the db
        self.__write_to_db("covid", data, connection=conn, cursor=cur)
        conn.commit()

        # Next, loop over all days to fill gaps in the table and calculate the accumulated sum 
        # of various numbers over the last 7 days.

        day = self.first_date
        delta = dt.timedelta(days=1)

        # Initiate vectors with the numbers of the last 7 days.
        seven_day_pcr_test = [0,0,0,0,0,0,0]
        seven_day_pcr_pos = [0,0,0,0,0,0,0]
        seven_day_hosp_cnt = [0,0,0,0,0,0,0]
        seven_day_death_cnt = [0,0,0,0,0,0,0]

        # Start the loop.
        while day <= self.last_date:
            
            # Convert day to string.
            date_str = day.strftime("%Y-%m-%d")

            # Check if the date already has an entry in the covid table.
            if not self.exists(date_str, table="covid"):

                # Initialize variables
                pcr_test, pcr_pos = 0,0
                hosp_cnt, death_cnt = 0,0

                # Remove the first entry of the vectors
                seven_day_pcr_test = seven_day_pcr_test[1:7]
                seven_day_pcr_pos = seven_day_pcr_pos[1:7]
                seven_day_hosp_cnt = seven_day_hosp_cnt[1:7]
                seven_day_death_cnt = seven_day_death_cnt[1:7]

                # And append the value of today.
                seven_day_pcr_test.append(pcr_test)
                seven_day_pcr_pos.append(pcr_pos)
                seven_day_hosp_cnt.append(hosp_cnt)
                seven_day_death_cnt.append(death_cnt)

                # Check if the date falls within the pandemic and set pandemic variable accordingly.
                if day < pandemic_end and day > pandemic_start:
                    pandemic = 1
                else:
                    pandemic = 0

                # Write data to database.
                self.write_db("INSERT INTO covid VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (date_str,
                        pandemic,
                        pcr_test,
                        pcr_pos,
                        hosp_cnt,
                        death_cnt,
                        sum(seven_day_pcr_test),
                        sum(seven_day_pcr_pos),
                        sum(seven_day_hosp_cnt),
                        sum(seven_day_death_cnt)
                        ))
            else:
                
                # If the entry already exists, get the numbers from the database
                info = self.get_covid_info(day, connection=conn, cursor=cur)
                pcr_test = info["pcr_test"]
                pcr_pos = info["pcr_pos"]
                hosp_cnt = info["hosp_cnt"]
                death_cnt = info["death_cnt"]

                # Remove the first entry of the vectors
                seven_day_pcr_test = seven_day_pcr_test[1:7]
                seven_day_pcr_pos = seven_day_pcr_pos[1:7]
                seven_day_hosp_cnt = seven_day_hosp_cnt[1:7]
                seven_day_death_cnt = seven_day_death_cnt[1:7]

                # And append the value of today.
                seven_day_pcr_test.append(pcr_test)
                seven_day_pcr_pos.append(pcr_pos)
                seven_day_hosp_cnt.append(hosp_cnt)
                seven_day_death_cnt.append(death_cnt)    

                # Update the database entry with the cumulative values.
                self.write_db("""
                        UPDATE covid SET
                        seven_day_pcr_test = ?,
                        seven_day_pcr_pos = ?,
                        seven_day_hosp_cnt = ?,
                        seven_day_death_cnt = ?
                        WHERE id = ?""",
                        (sum(seven_day_pcr_test),
                        sum(seven_day_pcr_pos),
                        sum(seven_day_hosp_cnt),
                        sum(seven_day_death_cnt),
                        date_str))
            day += delta

        # Commit the changes.
        conn.commit()

        # Set the covid_info flag to True.
        self.covid_info = True
                

    def get_covid_info(self, date, connection=None, cursor=None):
        """
        Get the Covid details for date from the database. Returns a dictionary if an entry
        exists and None otherways.
        """

        cur,conn = self.__get_cursor(connection, cursor)
        result = self.query_db("SELECT * FROM covid WHERE id=?", (date,), connection=conn, cursor=cur)
        if connection == None:
            conn.close()
        if len(result) == 0:
            return(None)
        else:
            return({"pandemic": result[0][1],
                    "pcr_test": result[0][2],
                    "pcr_pos": result[0][3],
                    "hosp_cnt": result[0][4],
                    "death_cnt": result[0][5],
                    "seven_day_pcr_test": result[0][6],
                    "seven_day_pcr_pos": result[0][7],
                    "seven_day_hosp_cnt": result[0][8],
                    "seven_day_death_cnt": result[0][9]})      

                


    def load_weather_info(self, filename, data_filter=None, rebuild=False, connection=None, cursor=None):
        """
        Load the weather info from a .csv file. A data_filter can be specified to include only 
        filtered columns in the database.
        """
        cur,conn = self.__get_cursor(connection, cursor)

        # Create a new column for weather data.
        self.__create_column("dates", "weather", "text", rebuild=rebuild, connection=conn, cursor=cur)

        # Initiate the filter
        filter_dict = {}
 
        # If no filter is given, read the column names from the first line of the .csv file
        # and use them as filter.
        if data_filter == None:
            with open(filename, 'r') as f:
                linereader = csv.reader(f)
                data_filter = next(linereader)

        # Open the .csv file and iterate over all rows.        
        with open(filename, 'r') as f:
            linereader = csv.reader(f)
            firstline = True
            for row in linereader:  

                # Treat the first line differently because it contains column names 
                if firstline:

                    # Make sure the most important values are contained in the data
                    if set(data_filter).issubset(set(row)):

                        # Write for each element of the data_filter the corresponding row number to
                        # filter_dict.
                        for key in data_filter:
                            filter_dict.update({key:row.index(key)})
                        firstline = False
                    else:

                        # If not, print a message and raise an Error.
                        print("""
The csv file seems to be malformed. Make sure to download the weather data as a .csv file from the CDO database of the NOAA at https://www.ncei.noaa.gov/cdo-web/.
                        """)
                        conn.close()
                        raise(ValueError)
                else:

                    # Initialize weather dictionary
                    weather = {}

                    # Replace empty strings in row with 0
                    row[:] = [x if x != "" else 0 for x in row]
                    
                    # Iterate over filter elements and try to convert corresponding entries to float.
                    # If that fails, leave them as string. Finally add the key, value pair to the
                    # weather dictionary.
                    for k in range(len(data_filter)):                        
                        try:
                            weather.update({data_filter[k]: float(row[filter_dict[data_filter[k]]])})
                        except ValueError:
                            weather.update({data_filter[k]: row[filter_dict[data_filter[k]]]})

                    # If average, minimum and maximum temperature are in the filter and the average
                    # temperature is 0, replace the average temperature with the mean of min and max
                    # temperature.                                
                    if set(["TAVG", "TMIN", "TMAX"]).issubset(data_filter) and weather["TAVG"] == 0:
                        weather["TAVG"] = round((weather["TMAX"]-weather["TMIN"])/2,1)

                    # Get the date as string.
                    date_str = weather["DATE"]

                    # Check if the date is in the dates table
                    if self.exists(date_str, table="dates"):

                        # Write the new entry if it does not exist yet or the rebuild flag is set.
                        if rebuild or len(self.get_weather(date_str)) == 0:
                            self.write_db("UPDATE dates SET weather = ? WHERE id = ?", (json.dumps(weather), date_str), connection=conn, cursor=cur, commit=True)
                            conn.commit()

        self.weather_info = True
        if connection == None:
            conn.close()


    def query_db(self, query, data=(), connection=None, cursor=None):
        """ Perform a SQL query on the database and return results """
        cur,conn = self.__get_cursor()
        cur.execute(query, data)
        result = cur.fetchall()
        if connection == None:
            conn.close()

        return(result)


    def write_db(self, query, data=(), commit = False, connection=None, cursor=None):
        """ Perform a SQL command on the database """
        cur,conn = self.__get_cursor()
        cur.execute(query, data)
        if commit or connection == None:
            conn.commit()
        if connection == None:
            conn.close()
   

    def get_type_stats(self, date=None, start_ts=None, end_ts=None):
        """
        Returns a statistic of the 911 calls between two timestamps. I.e. the number of calls
        for the different types. Returns a dictionary.
        """
        if date != None:
            start_ts, end_ts = self.__date_to_ts(date)
        elif start_ts==None or end_ts==None:
            print("You need to provide a date or two timestamps")
            return({})

        query = "SELECT type FROM calls WHERE timestamp >= ? AND timestamp < ?;"

        result = self.query_db(query, data=(start_ts,end_ts,))
        type_list = []
        for row in result:
            type_list.append(row[0])
        return(Counter(type_list))


    def get_date_details(self, date):
        """
        Retrieve the details for a given date from the database.
        """
        query = "SELECT details FROM dates WHERE id = ?"
        result = self.query_db(query, data=(date,))[0][0]
        return(json.loads(result))


    def get_weather(self, date, connection=None, cursor=None):
        """
        Get weather details for date. Returns an empty dictionary if no entry exists.
        """
        cur,conn = self.__get_cursor(connection, cursor)
        result = self.query_db("SELECT weather FROM dates WHERE id=?", (date,), connection=conn, cursor=cur)
        if connection == None:
            conn.close()
        if result[0][0] == None:
            return({})
        else:
            return(json.loads(result[0][0]))                 
        

    def get_hourly_stats(self, date=None, start_ts=None, end_ts=None):
        """
        Returns a hourly statistic of the 911 calls between two timestamps. I.e. the number of calls at different hours of the day. Returns a tuple.
        """
        if date != None:
            start_ts, end_ts = self.__date_to_ts(date)
        elif start_ts==None or end_ts==None:
            print("You need to provide a date or two timestamps")
            return({})
        
        query = "SELECT hour FROM calls WHERE timestamp >= ? AND timestamp < ?;"
        result = self.query_db(query, data=(start_ts,end_ts,))
        
        rlist = []
        for row in result:
            rlist.append(row[0])

        stat = []
        for h in range(24):
            stat.append(rlist.count(h))

        return(stat)
            

    def count_daily(self, date=None, date_tuple=None, timestamp=None):
        """
        Count the number of 911 calls on a given date. Dates can be provided as strings of the
        form 'YYYY-MM-DD', as tuples of the form (YYYY, MM, DD) or as a timestamp.
        If the date is provided as a timestamp, the result will be the number of calls
        on the day to which the timestamp belongs.
        """

        if date != None:
            day = dt.datetime.strptime(date, "%Y-%m-%d").date()
        elif date_tuple != None:
            day = dt.datetime(year = date_tuple[0], month = date_tuple[1], day = date_tuple[2]).date()
        elif timestamp != None:
            day = dt.date.fromtimestamp(timestamp)
        else:
            print("You need to provide at least one argument")
            return(0)

        start_ts = dt.datetime(day.year, day.month, day.day, 0,0,0).timestamp()
        end_ts = dt.datetime(day.year, day.month, day.day, 23,59,59).timestamp()
                             
        result = self.count_between(start_ts=start_ts, end_ts=end_ts)
        return(result)


    def count_between(self, start=None, end=None, start_ts=None, end_ts=None):
        """
        Count the number of 911 calls between two timepoints.
        Times must be given either as a string of the form 'YYYY-MM-DD HH:MM:SS' or as a timestamp.
        """
        
        if start == None and start_ts == None:
            start_ts = 0
        elif start_ts == None:
            try:
                start_ts = dt.datetime.strptime(start, "%Y-%m-%d %H:%M:%S").timestamp()
            except ValueError as e:
                print(e)
            
        if end == None and end_ts == None:
            end_ts = int(dt.datetime.now().timestamp())
        elif end_ts == None:
            try:
                end_ts = dt.datetime.strptime(end, "%Y-%m-%d %H:%M:%S").timestamp()
            except ValueError as e:
                print(e)
            
        query = """
        SELECT COUNT(*) FROM calls WHERE timestamp >= ? AND timestamp < ?;
        """

        result = self.query_db(query, (start_ts, end_ts,))

        return(result[0][0])
        
            
