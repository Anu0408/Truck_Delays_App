import pandas as pd
import configparser
import os

# Load configurations
config = configparser.ConfigParser()
config.read('C:/Users/anucv/OneDrive/Desktop/AI and ML training/Machine_Learning/TRUCK_DELAY_CLASSIFICATION_PROJECT/Config/config.ini')

# Fetching cleaned data path from config file
cleaned_data_path = config['PATHS']['cleaned_data_path']

class DataPreparation:
    def __init__(self, traffic_df, schedule_df, weather_df, trucks_df, drivers_df, routes_df):
        self.traffic_df = traffic_df
        self.schedule_df = schedule_df
        self.weather_df = weather_df
        self.trucks_df = trucks_df
        self.drivers_df = drivers_df
        self.routes_df = routes_df
        
    def prepare_data(self):
        # Drop unnecessary columns and duplicates
        self.weather_df.drop(columns=['chanceofrain', 'chanceoffog', 'chanceofsnow', 'chanceofthunder'], inplace=True)
        self.weather_df.drop_duplicates(subset=['city_id', 'date', 'hour'], inplace=True)
        self.schedule_df.drop_duplicates(subset=['truck_id', 'route_id', 'departure_date'], inplace=True)
        self.trucks_df.drop_duplicates(subset=['truck_id'], inplace=True)
        self.drivers_df.drop_duplicates(subset=['driver_id'], inplace=True)
        self.routes_df.drop_duplicates(subset=['route_id', 'destination_id', 'origin_id'], inplace=True)
        
        # Convert hour to datetime
        self.weather_df['hour'] = self.weather_df['hour'].apply(lambda x: f"{int(x):04d}")
        self.weather_df['custom_date'] = pd.to_datetime(self.weather_df['date'] + ' ' + self.weather_df['hour'])
        
        # Merging and feature engineering
        self.schedule_df['estimated_arrival'] = pd.to_datetime(self.schedule_df['estimated_arrival']).dt.ceil("6H")
        self.schedule_df['departure_date'] = pd.to_datetime(self.schedule_df['departure_date']).dt.floor("6H")
        
        # Create date ranges
        self.schedule_df['date'] = self.schedule_df.apply(lambda row: pd.date_range(start=row['departure_date'], end=row['estimated_arrival'], freq='6H'), axis=1)
        self.schedule_df = self.schedule_df.explode('date')
        
        # Merge with weather data
        schedule_weather_df = pd.merge(self.schedule_df, self.weather_df, left_on=['route_id', 'date'], how='left')
        
        # Custom aggregation for scheduled weather
        def custom_mode(x):
            return x.mode().iloc[0] if not x.empty else None

        schedule_weather_grp = schedule_weather_df.groupby(['truck_id', 'route_id'], as_index=False).agg(
            route_avg_temp=('temp', 'mean'),
            route_avg_wind_speed=('wind_speed', 'mean'),
            route_avg_precip=('precip', 'mean'),
            route_avg_humidity=('humidity', 'mean'),
            route_avg_visibility=('visibility', 'mean'),
            route_avg_pressure=('pressure', 'mean'),
            route_description=('description', custom_mode)
        )
        
        # Merging again
        schedule_weather_merged = pd.merge(self.schedule_df, schedule_weather_grp, on=['truck_id', 'route_id'], how='left')
        
        # Merging with traffic data
        hourly_exploded_schedule_df = schedule_weather_merged.assign(custom_date=[pd.date_range(start, end, freq='H') for start, end in zip(schedule_weather_merged['departure_date'], schedule_weather_merged['estimated_arrival'])]).explode('custom_date')
        
        scheduled_traffic = pd.merge(hourly_exploded_schedule_df, self.traffic_df, left_on=['route_id', 'custom_date'], how='left')
        
        # Custom aggregation for accidents
        def custom_agg(values):
            return 1 if any(values == 1) else 0
        
        scheduled_route_traffic = scheduled_traffic.groupby(['truck_id', 'route_id'], as_index=False).agg(
            avg_no_of_vehicles=('no_of_vehicles', 'mean'),
            accident=('accident', custom_agg)
        )
        
        # Final merge
        final_merge = pd.merge(scheduled_route_traffic, self.trucks_df, on='truck_id', how='left')
        final_merge = pd.merge(final_merge, self.drivers_df, left_on='truck_id', right_on='vehicle_no', how='left')
        
        # Check for nighttime involvement
        def has_midnight(start, end):
            return int(start.date() != end.date())

        final_merge['is_midnight'] = final_merge.apply(lambda row: has_midnight(row['departure_date'], row['estimated_arrival']), axis=1)

        print("Final merged data shape:", final_merge.shape)


class DataPreparationComponent:
    def __init__(self):
        self.cleaned_data_path = cleaned_data_path

    def fetch_data(self):
        # Fetching cleaned data
        traffic_df = pd.read_csv(os.path.join(self.cleaned_data_path, 'cleaned_traffic.csv'))
        truck_schedule_df = pd.read_csv(os.path.join(self.cleaned_data_path, 'cleaned_truck_schedule.csv'))
        city_weather_df = pd.read_csv(os.path.join(self.cleaned_data_path, 'cleaned_city_weather.csv'))
        trucks_df = pd.read_csv(os.path.join(self.cleaned_data_path, 'cleaned_trucks.csv'))
        drivers_df = pd.read_csv(os.path.join(self.cleaned_data_path, 'cleaned_drivers.csv'))
        routes_df = pd.read_csv(os.path.join(self.cleaned_data_path, 'cleaned_routes.csv'))
        routes_weather_df = pd.read_csv(os.path.join(self.cleaned_data_path, 'cleaned_routes_weather.csv'))

        # Print shapes and datatypes
        print("Data Shapes and Data Types:")
        print("Traffic DataFrame Shape:", traffic_df.shape, "Data Types:\n", traffic_df.dtypes)
        print("Truck Schedule DataFrame Shape:", truck_schedule_df.shape, "Data Types:\n", truck_schedule_df.dtypes)
        print("City Weather DataFrame Shape:", city_weather_df.shape, "Data Types:\n", city_weather_df.dtypes)
        print("Trucks DataFrame Shape:", trucks_df.shape, "Data Types:\n", trucks_df.dtypes)
        print("Drivers DataFrame Shape:", drivers_df.shape, "Data Types:\n", drivers_df.dtypes)
        print("Routes DataFrame Shape:", routes_df.shape, "Data Types:\n", routes_df.dtypes)
        print("Routes Weather DataFrame Shape:", routes_weather_df.shape, "Data Types:\n", routes_weather_df.dtypes)

        return {
            'traffic': traffic_df,
            'truck_schedule': truck_schedule_df,
            'city_weather': city_weather_df,
            'trucks': trucks_df,
            'drivers': drivers_df,
            'routes': routes_df,
            'routes_weather': routes_weather_df,
        }

    def prepare_data(self):
        data = self.fetch_data()

        # Merging and processing logic
        cleaned_traffic_df = data['traffic']
        cleaned_truck_schedule_df = data['truck_schedule']
        cleaned_city_weather_df = data['city_weather']
        cleaned_trucks_df = data['trucks']
        cleaned_drivers_df = data['drivers']
        cleaned_routes_df = data['routes']

        # Ensure the date_time column is available
        if 'date_time' not in cleaned_traffic_df.columns:
            print("Error: 'date_time' column not found in cleaned_traffic_df")
            return None
        
        # Convert date columns to datetime
        try:
            cleaned_traffic_df['date_time'] = pd.to_datetime(cleaned_traffic_df['date_time'])
            cleaned_truck_schedule_df['departure_date'] = pd.to_datetime(cleaned_truck_schedule_df['departure_date'])
            cleaned_truck_schedule_df['estimated_arrival'] = pd.to_datetime(cleaned_truck_schedule_df['estimated_arrival'])
        except Exception as e:
            print(f"Error converting date columns: {e}")
            return None

        # Round to nearest hour
        cleaned_truck_schedule_df['departure_date'] = cleaned_truck_schedule_df['departure_date'].dt.round('h')
        cleaned_truck_schedule_df['estimated_arrival'] = cleaned_truck_schedule_df['estimated_arrival'].dt.round('h')

        # Create nearest hour schedule DataFrame
        nearest_hour_schedule_df = cleaned_truck_schedule_df.assign(
            custom_date=[pd.date_range(start, end, freq='h') for start, end in zip(
                cleaned_truck_schedule_df['departure_date'], cleaned_truck_schedule_df['estimated_arrival']
            )]
        ).explode('custom_date', ignore_index=True)

        # Prepare weather data
        city_weather_df = cleaned_city_weather_df[['city_id', 'date_time', 'temp', 'wind_speed', 'description', 'precip', 'humidity', 'visibility', 'pressure']]

        # Check if origin_id and destination_id columns are in truck schedule
        if 'origin_id' not in cleaned_routes_df.columns or 'destination_id' not in cleaned_routes_df.columns:
            print("Error: 'origin_id' or 'destination_id' not found in routes_df")
            return None

        # Merge with origin and destination weather
        origin_weather_merge = pd.merge(
            nearest_hour_schedule_df,
            city_weather_df,
            left_on=['origin_id', 'custom_date'],
            right_on=['city_id', 'date_time'],
            how='left',
            suffixes=('', '_origin')
        )

        destination_weather_merge = pd.merge(
            origin_weather_merge,
            city_weather_df,
            left_on=['destination_id', 'custom_date'],
            right_on=['city_id', 'date_time'],
            how='left',
            suffixes=('', '_destination')
        )

        print("Data preparation completed successfully.")

        return destination_weather_merge

# Usage example
if __name__ == '__main__':
    component = DataPreparationComponent()
    prepared_data = component.prepare_data()
