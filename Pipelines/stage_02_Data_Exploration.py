import os
import sys
import configparser
import pandas as pd
from sqlalchemy import create_engine

# Add src directory to system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.components.Data_Exploration import DataExplorationComponent

# Load configuration
config = configparser.RawConfigParser()
config.read('C:/Users/anucv/OneDrive/Desktop/AI and ML training/Machine_Learning/TRUCK_DELAY_CLASSIFICATION_PROJECT/Config/config.ini')

# Database connection parameters
db_host = config.get('database', 'host')
db_port = config.get('database', 'port')
db_name = config.get('database', 'name')
db_user = config.get('database', 'user')
db_password = config.get('database', 'password')

# Create database connection
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# List of dataframes to explore
dataframes = {
    'city_weather': 'SELECT * FROM city_weather;',
    'drivers': 'SELECT * FROM drivers_table;',
    'routes': 'SELECT * FROM routes_table;',
    'routes_weather': 'SELECT * FROM routes_weather;',
    'traffic': 'SELECT * FROM traffic_table;',
    'trucks': 'SELECT * FROM trucks_table;',
    'truck_schedule': 'SELECT * FROM truck_schedule_table;'
}

STAGE_NAME = "Data Exploration"

class DataExplorationPipeline:
    def __init__(self):
        self.engine = engine

    def run_exploration_pipeline(self):
        for df_name, query in dataframes.items():
            print(f">>>>>> Exploration started for: {df_name} <<<<<<")

            # Read data into a DataFrame
            df = pd.read_sql_query(query, self.engine)

            # Create the component and explore the DataFrame
            exploration_component = DataExplorationComponent(df_name, df)
            exploration_component.run()

            print(f">>>>>> Exploration completed for: {df_name} <<<<<<\n")

if __name__ == '__main__':
    try:
        print(">>>>>> Stage started <<<<<< :", STAGE_NAME)
        pipeline = DataExplorationPipeline()
        pipeline.run_exploration_pipeline()
        print(">>>>>> Stage completed <<<<<<", STAGE_NAME)
    except Exception as e:
        print(e)
        raise e
