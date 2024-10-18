import os
import sys
import configparser
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime  # Import datetime to handle date
from src.components.Data_Cleaning import DataCleaning

# Add src directory to system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load configuration
config = configparser.RawConfigParser()
config.read('C:/Users/anucv/OneDrive/Desktop/AI and ML training/Machine_Learning/TRUCK_DELAY_CLASSIFICATION_PROJECT/Config/config.ini')

# Database connection parameters
db_host = config.get('database', 'host')
db_port = config.get('database', 'port')
db_name = config.get('database', 'name')
db_user = config.get('database', 'user')
db_password = config.get('database', 'password')

# Path to store cleaned data (for now, we won’t store it in the feature store)
cleaned_data_path = config.get('HOPSWORKS', 'cleaned_data_path')

# Test database connection
try:
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    connection = engine.connect()
    print("Database connection successful")
    
    # Optional: Fetch and display some data from the database to verify retrieval
    test_query = 'SELECT * FROM city_weather LIMIT 5;'
    test_df = pd.read_sql_query(test_query, engine)
    print("Sample data from city_weather:")
    print(test_df.head())
    print(f"Sample data shape: {test_df.shape}")

except Exception as e:
    print(f"Error during data retrieval: {e}")
    sys.exit()  # Exit if the connection or data retrieval fails

# List of dataframes to clean
dataframes = {
    'city_weather': 'SELECT * FROM city_weather;',
    'drivers': 'SELECT * FROM drivers_table;',
    'routes': 'SELECT * FROM routes_table;',
    'routes_weather': 'SELECT * FROM routes_weather;',
    'traffic': 'SELECT * FROM traffic_table;',
    'trucks': 'SELECT * FROM trucks_table;',
    'truck_schedule': 'SELECT * FROM truck_schedule_table;'
}

# Numerical features for each dataframe
numerical_features_dict = {
    'city_weather': ['hour', 'temp', 'wind_speed', 'humidity', 'pressure'],
    'drivers': ['age', 'experience', 'ratings', 'average_speed_mph'],
    'routes': ['distance', 'average_hours'],
    'routes_weather': ['temp', 'wind_speed', 'humidity', 'pressure'],
    'traffic': ['no_of_vehicles', 'accident'],
    'trucks': ['truck_age']
}

STAGE_NAME = "Data Cleaning"

class DataCleaningPipeline:
    def __init__(self):
        self.engine = engine

    def run_cleaning_pipeline(self):
        for df_name, query in dataframes.items():
            print(f">>>>>> Cleaning started for: {df_name} <<<<<<")

            # Read data into a DataFrame
            try:
                df = pd.read_sql_query(query, self.engine)
                print(f"Fetched {df.shape[0]} rows and {df.shape[1]} columns from {df_name}")
            except Exception as e:
                print(f"Error retrieving data for {df_name}: {e}")
                continue  # Skip this dataframe if data retrieval fails

            # Create an index column starting from 1
            df['index'] = range(1, len(df) + 1)

            # Create event_date column with the current date
            today_date = datetime.now().strftime('%Y-%m-%d')
            df['event_date'] = pd.to_datetime(today_date)

            # Define numerical features for the current dataframe
            numerical_features = numerical_features_dict.get(df_name, [])

            # Create a DataCleaning object and clean the DataFrame
            cleaning_obj = DataCleaning(df, df_name)
            cleaned_df = cleaning_obj.full_cleaning_process(numerical_features)

            # Define output path for cleaned data
            output_path = os.path.join(cleaned_data_path, f'cleaned_{df_name}.csv')
            cleaned_df.to_csv(output_path, index=False)
            print(f"Cleaned data saved to: {output_path}")

            print(f">>>>>> Cleaning completed for: {df_name} <<<<<<\n")

if __name__ == '__main__':
    try:
        print(">>>>>> Stage started <<<<<< :", STAGE_NAME)
        pipeline = DataCleaningPipeline()
        pipeline.run_cleaning_pipeline()
        print(">>>>>> Stage completed <<<<<<", STAGE_NAME)
    except Exception as e:
        print(f"Pipeline error: {e}")
        raise e
