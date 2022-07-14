import requests
import pandas as pd
import sqlite3
from secrets import token

api_key = token

cities_dict = {
    "Valencia": "ES",
    "Sevilla": "ES",
    "Barcelona": "ES",
    "Madrid": "ES"
}

coords_dict = {}
for k, v in cities_dict.items():
    #print(k, v[0])
    URL = "https://api.openweathermap.org/data/2.5/weather?q={0},{1}&appid={2}" \
        .format(k, v, api_key)
    response = requests.get(url = URL)
    data = response.json()
    
    coords_dict[k] = [
        data["coord"]["lon"], 
        data["coord"]["lat"]]


measures_dict = {}
for k, v in coords_dict.items():
    
    URL = "https://api.openweathermap.org/data/2.5/weather?lat={1}&lon={0}&appid={2}&units=metric" \
        .format(v[0], v[1], api_key)
    PARAMS = {}
    response = requests.get(url = URL, params = PARAMS)
    data = response.json()
        
    measures_dict[k] = {
        "weather_main": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "main_temp": data["main"]["temp"],
        "main_feels_like": data["main"]["feels_like"],
        "main_temp_min": data["main"]["temp_min"],
        "main_temp_max": data["main"]["temp_max"],
        "main_pressure": data["main"]["pressure"],
        "main_humidity": data["main"]["humidity"],
        "visibility": data["visibility"],
        "wind_speed": data["wind"]["speed"],
        "wind_deg": data["wind"]["deg"],
        "sys_sunrise": data["sys"]["sunrise"],
        "sys_sunset": data["sys"]["sunset"],
        "sys_country": data["sys"]["country"],
        "coord_lon": data["coord"]["lon"],
        "coord_lat": data["coord"]["lat"]
    }

    lon = v[0]
    lat = v[1]

    pollution_URL = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"

    response = requests.get(url = pollution_URL)
    data = response.json()   

    measures_dict[k]["components_co"] = data["list"][0]["components"]["co"]
    measures_dict[k]["components_no"] = data["list"][0]["components"]["no"]
    measures_dict[k]["components_no2"] = data["list"][0]["components"]["no2"]
    measures_dict[k]["components_o3"] = data["list"][0]["components"]["o3"]
    measures_dict[k]["components_so2"] = data["list"][0]["components"]["so2"]
    measures_dict[k]["components_pm2_5"] = data["list"][0]["components"]["pm2_5"]
    measures_dict[k]["components_pm10"] = data["list"][0]["components"]["pm10"]
    measures_dict[k]["components_nh3"] = data["list"][0]["components"]["nh3"]
    measures_dict[k]["date_time"] = data["list"][0]["dt"]

temp_df = pd.DataFrame(measures_dict)

temp_df = temp_df.T.reset_index()
temp_df.rename(columns={"index": "city"}, inplace = True)

temp_df['date_time'] = pd.to_datetime(temp_df['date_time'], unit='s')

conn = sqlite3.connect("temperatures_db.sqlite3")

c = conn.cursor()

create_measures_query = """
    CREATE TABLE IF NOT EXISTS temperatures (
        [date_time] TEXT, 
        [city] TEXT,
        [weather_main] TEXT,
        [weather_description] TEXT,
        [main_temp] INTEGER,
        [main_feels_like] INTEGER,
        [main_temp_min] INTEGER,
        [main_temp_max] INTEGER,
        [main_pressure] INTEGER,
        [main_humidity] INTEGER,
        [visibility] INTEGER,
        [wind_speed] INTEGER,
        [wind_deg] INTEGER,
        [sys_sunrise] INTEGER,
        [sys_sunset] INTEGER,
        [sys_country] INTEGER,
        [coord_lon] INTEGER,
        [coord_lat] INTEGER,
        [components_co] INTEGER,
        [components_no] INTEGER,
        [components_no2] INTEGER,
        [components_o3] INTEGER,
        [components_so2] INTEGER,
        [components_pm2_5] INTEGER,
        [components_pm10] INTEGER,
        [components_nh3] INTEGER)
        """

c.execute(create_measures_query)
conn.commit()

temp_df.to_sql('temperatures', conn, if_exists = 'append', index = False)