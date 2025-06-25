from airflow import DAG
from airflow.providers.https.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

#Latitude and Longitude of the location (London in this case)
Latitude = '51.5074'
Longitude = '-0.1278'
POST_GRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

#DAG
with DAG(dag_id = 'weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    @task()
    def extract_weather_data():
        """Extracts weather data from the Open Meteo API using airflow connection"""
        #3 Use http hook to get connection details from airflow connection

        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        #Build the API endpoint
        # https://api.open-metro.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f'/v1/forecast?latitude={Latitude}&longitude={Longitude}&current_weather=true'

        ## make the request via the hhtp hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")
        
    @task()
    def transform_weather_data(weather_data):
        """Transforms the weather data to extract relevant information"""
        # Extract relevant information from the API response
        current_weather = weather_data['current_weather']
        temperature = current_weather['temperature']
        windspeed = current_weather['windspeed']
        weathercode = current_weather['weathercode']
        
        # Create a dictionary with the transformed data
        transformed_data = {
            'latitude': Latitude,
            'longitude': Longitude,
            'temperature': temperature,
            'windspeed': windspeed,
            'weathercode': weathercode
        }
        
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        """Loads the transformed data into a PostgreSQL database"""
        # Create a Postgres hook
        pg_hook = PostgresHook(postgres_conn_id=POST_GRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()  

        #create the table if it doesn't exist
        cursor.execute( """
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        
        # Insert the transformed data into the table
        cursor.execute ( """
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, weathercode)
            VALUES (%s, %s, %s, %s, %s);
        """, (transformed_data['latitude'],
              transformed_data['longitude'], 
              transformed_data['temperature'], 
              transformed_data['windspeed'], 
              transformed_data['weathercode']
            ))
        
        conn.commit()
        cursor.close()

    ## DAG Workflow - ETL pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
        

    