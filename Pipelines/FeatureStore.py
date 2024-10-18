import os
import configparser
import pandas as pd
from datetime import datetime
import hopsworks

class FeatureStorePipeline:
    def __init__(self, config_file):
        # Load configuration
        config = configparser.ConfigParser()
        config.read(config_file)

        # Get Hopsworks URL and API key
        hopsworks_url = config['Hopsworks']['hopsworks_url']
        api_key = config['Hopsworks']['api_key']

        # Connect to Hopsworks
        project = hopsworks.login(api_key=api_key, project="default")  # Use "default" or your project name
        self.fs = project.get_feature_store()  # Initialize Hopsworks feature store

    def create_feature_group(self, df_name, df, primary_key):
        # Set event_time to today's date
        df['event_time'] = pd.Timestamp.now()

        # Create Feature Group
        feature_group = self.fs.get_or_create_feature_group(
            name=f"{df_name}_fg",
            version=1,
            description=f"{df_name} data",
            primary_key=primary_key,
            event_time='event_time',
            online_enabled=False
        )
        
        # Insert DataFrame to feature group
        self.fs.insert(feature_group, df)

        return feature_group

    def update_feature_descriptions(self, feature_group, descriptions):
        for desc in descriptions:
            self.fs.update_feature_description(feature_group, desc["name"], desc["description"])

    def configure_statistics(self, feature_group):
        statistics_config = {
            "enabled": True,
            "histograms": True,
            "correlations": True
        }
        self.fs.update_statistics_config(feature_group, statistics_config)
        self.fs.compute_statistics(feature_group)

    def run_pipeline(self):
        # List of dataframes with SQL queries, primary keys, and descriptions
        dataframes = {
            'drivers': {
                'query': 'SELECT * FROM drivers_table;',
                'primary_key': ['driver_id'],
                'descriptions': [
                    {"name": "driver_id", "description": "Unique identification for each driver"},
                    {"name": "name", "description": "Name of the truck driver"},
                    {"name": "gender", "description": "Gender of the truck driver"},
                    {"name": "age", "description": "Age of the truck driver"},
                    {"name": "experience", "description": "Experience of the truck driver in years"},
                    {"name": "driving_style", "description": "Driving style of the truck driver, conservative or proactive"},
                    {"name": "ratings", "description": "Average rating of the truck driver on a scale of 1 to 5"},
                    {"name": "vehicle_no", "description": "The number of the driver’s truck"},
                    {"name": "average_speed_mph", "description": "Average speed of the truck driver in miles per hour"},
                    {"name": "event_time", "description": "Event time (today's date)"}
                ]
            },
            'trucks': {
                'query': 'SELECT * FROM trucks_table;',
                'primary_key': ['truck_id'],
                'descriptions': [
                    {"name": "truck_id", "description": "Unique identification for each truck"},
                    {"name": "model", "description": "Model of the truck"},
                    {"name": "year", "description": "Year of manufacture"},
                    {"name": "capacity", "description": "Carrying capacity of the truck"},
                    {"name": "event_time", "description": "Event time (today's date)"}
                ]
            },
            'routes': {
                'query': 'SELECT * FROM routes_table;',
                'primary_key': ['route_id'],
                'descriptions': [
                    {"name": "route_id", "description": "Unique identification for each route"},
                    {"name": "start_location", "description": "Starting location of the route"},
                    {"name": "end_location", "description": "Ending location of the route"},
                    {"name": "distance", "description": "Distance of the route in miles"},
                    {"name": "event_time", "description": "Event time (today's date)"}
                ]
            },
            'truck_schedule': {
                'query': 'SELECT * FROM truck_schedule_table;',  # Add the appropriate SQL query here
                'primary_key': ['truck_id', 'route_id'],
                'descriptions': [
                    {"name": "truck_id", "description": "Unique identification number for each truck"},
                    {"name": "route_id", "description": "Unique identification number for each route"},
                    {"name": "departure_date", "description": "The date and time when the truck departs"},
                    {"name": "estimated_arrival", "description": "The estimated arrival date and time at the destination"},
                    {"name": "delay", "description": "The delay in minutes for the scheduled trip"},
                    {"name": "event_date", "description": "Event date related to the truck's schedule"},
                    {"name": "index", "description": "Index of the DataFrame entry"}
                ]
            },
            'city_weather': {
                'query': 'SELECT * FROM city_weather_table;',  # Replace with actual SQL query
                'primary_key': ['city_id', 'date'],
                'descriptions': [
                    {"name": "city_id", "description": "Unique identification for each city"},
                    {"name": "date", "description": "Date of the weather data"},
                    {"name": "temp", "description": "Temperature in degrees"},
                    {"name": "humidity", "description": "Humidity percentage"},
                    {"name": "wind_speed", "description": "Wind speed in miles per hour"},
                    {"name": "precip", "description": "Precipitation amount"},
                    {"name": "visibility", "description": "Visibility in miles"},
                    {"name": "pressure", "description": "Atmospheric pressure"},
                    {"name": "chanceofrain", "description": "Probability of rain"},
                    {"name": "event_time", "description": "Event time (today's date)"}
                ]
            },
            'routes_weather': {
                'query': 'SELECT * FROM routes_weather_table;',  # Replace with actual SQL query
                'primary_key': ['route_id', 'date'],
                'descriptions': [
                    {"name": "route_id", "description": "Unique identification for each route"},
                    {"name": "date", "description": "Date of the weather data"},
                    {"name": "temp", "description": "Temperature in degrees"},
                    {"name": "precip", "description": "Precipitation amount"},
                    {"name": "wind_speed", "description": "Wind speed in miles per hour"},
                    {"name": "visibility", "description": "Visibility in miles"},
                    {"name": "event_time", "description": "Event time (today's date)"}
                ]
            },
            'traffic': {
                'query': 'SELECT * FROM traffic_table;',  # Replace with actual SQL query
                'primary_key': ['traffic_id', 'timestamp'],
                'descriptions': [
                    {"name": "traffic_id", "description": "Unique identification for each traffic record"},
                    {"name": "timestamp", "description": "Timestamp of the traffic data"},
                    {"name": "route_id", "description": "Unique identification for each route"},
                    {"name": "congestion_level", "description": "Level of congestion on the route"},
                    {"name": "average_speed", "description": "Average speed in miles per hour"},
                    {"name": "event_time", "description": "Event time (today's date)"}
                ]
            }
        }

        for df_name, details in dataframes.items():
            print(f">>>>>> Processing feature group for: {df_name} <<<<<<")
            try:
                # Read data into a DataFrame
                df = pd.read_sql_query(details['query'], self.fs.engine)  # Adjust the engine reference as needed
                print(f"Fetched {df.shape[0]} rows and {df.shape[1]} columns from {df_name}")
                
                # Create feature group
                feature_group = self.create_feature_group(df_name, df, details['primary_key'])
                
                # Update descriptions
                self.update_feature_descriptions(feature_group, details['descriptions'])
                
                # Configure statistics
                self.configure_statistics(feature_group)

            except Exception as e:
                print(f"Error processing {df_name}: {e}")

if __name__ == '__main__':
    # Specify the path to your config file
    config_file = "config.ini"  # Update with your actual config file path
    
    try:
        pipeline = FeatureStorePipeline(config_file)
        pipeline.run_pipeline()
    except Exception as e:
        print(f"Pipeline error: {e}")
        raise e
