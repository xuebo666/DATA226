from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
import pandas as pd

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(latitude, longitude):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "Asia/Shanghai"
    }
    response = requests.get(url, params=params)
    return response.json()

@task
def transform(data, latitude, longitude):
    df = pd.DataFrame({
        "Date": data["daily"]["time"],
        "Temp_max": data["daily"]["temperature_2m_max"],
        "Temp_min": data["daily"]["temperature_2m_min"],
        "Precipitation": data["daily"]["precipitation_sum"],
        "Weather_code": data["daily"]["weather_code"]
    })
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    records = []
    for _, row in df.iterrows():
        records.append([
            latitude,
            longitude,
            str(row["Date"]),
            row["Temp_max"],
            row["Temp_min"],
            row["Precipitation"],
            row["Weather_code"]
        ])
    return records

@task
def load(records, target_table):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
            latitude      FLOAT,
            longitude     FLOAT,
            date          DATE,
            temp_max      FLOAT,
            temp_min      FLOAT,
            precipitation FLOAT,
            weather_code  INTEGER,
            PRIMARY KEY (latitude, longitude, date)
        );""")
        cur.execute(f"DELETE FROM {target_table}")
        for r in records:
            latitude, longitude, date, temp_max, temp_min, precipitation, weather_code = r
            print(date, "-", temp_max, temp_min)

            sql = f"""INSERT INTO {target_table}
            (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
            VALUES ({latitude}, {longitude}, '{date}', {temp_max}, {temp_min}, {precipitation}, {weather_code})"""
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id='HW3_WeatherETL',
    start_date=datetime(2026, 9, 12),
    catchup=False,
    tags=['ETL'],
    schedule='0 0 * * *'
) as dag:
    target_table = "raw.weather_daily"
    latitude = Variable.get("LATITUDE")
    longitude = Variable.get("LONGITUDE")

    data = extract(latitude, longitude)
    records = transform(data, latitude, longitude)
    load(records, target_table)
