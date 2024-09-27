# **1. Data Ingestion and Preparation**

  This project is the first part of a three-part series aimed at solving the truck delay prediction problem. In this initial phase, we will utilize PostgreSQL and MYSQL in AWS Redshift to store the data, perform data retrieval, and conduct basic exploratory data analysis (EDA). With Hopsworks feature store, we will build a pipeline that includes data processing feature engineering and prepare the data for model building.

### (a). Data Ingestion Approach
 
* Upload truck delay training data into github repo
* Create MYSQL or Postgress DB Server ( move to AWS RDS in future)
* Create Truck_delays database and create tables for each of csv files
* Develop data_ingestion component to ingest data from github to MYSQL

### (b). Data Exploration 

* Connect to Database
* Fetch Data from truck delay tables into Dataframes
* For each of the DataFrame -
  - Do basic checks of data ( info, describe, etc)
  - Chage Date column to datetime  ( ex : weather_df['date'] = pd.to_datetime(weather_df['date'])
* Data Analysis ( Important : For each of DF analyis - Clearly write your observations and recommandations)
  - Drivers
      * Histogram plots for numeric features
      * Scatter Plots ( Rating vs Average Speed)
      * Box Plot ( Driver Ratings by Gender)
  - Trucks
      * Histogram Plots for numeric features
      * Identify low milege Trucks and plot Trucks with age distribution
  - Routes
    * Histogram plots for numeric values
  - Traffic
    * Histogram plots for numeric values
    * Categorizes hours of the day into time periods.
      Example :
                if 300 <= hour < 600:
                       return 'Early Morning'

### (c). Data cleaning

* For Each of the DataFrame
   - Identify and treat null values
   - Identify and treat outliers

### (d). Create Feature groups in Feature Store

* Connect to feature store
* For each of the Data Frame,

  - Create event_time feature
      Example : drivers_df['event_time'] = pd.to_datetime('2024-09-19')
    
  - Create Feature group
    
               drivers_fg = fs.get_or_create_feature_group(
                   name="drivers_details_fg",                # Name of the feature group
                   version=1,                                # Version number
                   description="Drivers data",               # Description of the feature group
                   primary_key=['driver_id'],                # Primary key(s) for the feature group
                   event_time='event_time',                  # Event time column
                   online_enabled=False                      # Online feature store capability
               )
    
  - Insert DataFrame to feature group
    
                    drivers_fg.insert(drivers_df)
    
  - Create and Update Feature Group descriptions to each of the feature groups   
      
     
                  feature_descriptions_drivers = [
                  
                      {"name": "driver_id", "description": "unique identification for each driver"},
                      {"name": "name", "description": "name of the truck driver"},
                      {"name": "gender", "description": "gender of the truck driver"},
                      {"name": "age", "description": "age of the truck driver"},
                      {"name": "experience", "description": "experience of the truck driver in years"},
                      {"name": "driving_style", "description": "driving style of the truck driver, conservative or proactive"},
                      {"name": "ratings", "description": "average rating of the truck driver on a scale of 1 to 5"},
                      {"name": "vehicle_no", "description": "the number of the driver’s truck"},
                      {"name": "average_speed_mph", "description": "average speed of the truck driver in miles per hour"},
                      {"name": "event_time", "description": "dummy event time"}
                  
                  ]

                  for desc in feature_descriptions_drivers:
                      drivers_fg.update_feature_description(desc["name"], desc["description"])
                      

 - Configure statistics for the feature group

              # Configure statistics for the feature group
                  drivers_fg.statistics_config = {
                      "enabled": True,        # Enable statistics calculation
                      "histograms": True,     # Include histograms in the statistics
                      "correlations": True    # Include correlations in the statistics
                  }
                  
                  # Update the statistics configuration for the feature group
                  drivers_fg.update_statistics_config()
                  
                  # Compute statistics for the feature group
                  drivers_fg.compute_statistics()
### (e). Fetch Data from feature Store

  * Fetch data from each of the feature store for Truck dealy into data frames

           drivers_df_fg = fs.get_feature_group('drivers_details_fg', version=1)
           query = drivers_df_fg.select_all()
           drivers_df=query.read()
    
### (f). Data Preparation

   * For each of the dataframe
     
        - Drop event_time feature
        - Identify and drop duplicate rows
              weather :  [city_id,date,hour]
              route_weater: [route_id,date]
              trucks : [truck_id]
              drivers: [driver_id]
              route  : [route_id,destination_id,origin_id]
              schedule : [truck_id,route_id,depature_date]
        - Drop unnecessary columns
          
               * weather_weather : [chanceofrain,changceofogg,chanceofsnow,chanceofthunder]
               * weather : convert hour to a 4 diigit string format
                         convert hour to datetime format
                          combine data an hour to create a new datetime column 'custom_date' and insert at index 1
                
        - Featue Engineering
          * Scheuled data 
               * Merge Route_weather with Schedule data
                     *  create new data frame from scheduledf and add estimated_arrival and departure_date columns
                        estimated_arriaval= df[estimated_arrival].dt.ceil("6H')
                       departure_date= df[departure_date].dt.floor("6H")
                       
                         * Assign a new column 'date' using a list comprehension to generate date ranges between 'departure_date' and 'estimated_arrival' with a frequency of 6 hours
                         * This will create a list of date ranges for each row
                         * Explode the 'date' column to create separate rows for each date range
                         * Merge the resulant data frame with route_weather on route_id and date (left)
                         * Define a custom function to calculate mode
                                  def custom_mode(x):
                                      return x.mode().iloc[0]
                              
                         * Group by specified columns and aggregate
                              schedule_weather_grp = schduled_weather.groupby(['unique_id','truck_id','route_id'], as_index=False).agg(
                                  route_avg_temp=('temp','mean'),
                                  route_avg_wind_speed=('wind_speed','mean'),
                                  route_avg_precip=('precip','mean'),
                                  route_avg_humidity=('humidity','mean'),
                                  route_avg_visibility=('visibility','mean'),
                                  route_avg_pressure=('pressure','mean'),
                                  route_description=('description', custom_mode)
                              )
                         * Merge schedule df with schedule_weather_grp df

                              schedule_weather_merge=schedule_df.merge(schedule_weather_grp,on=['unique_id','truck_id','route_id'],how='left')
                 
                         * Find Origin and Destination city Weather

                              * take hourly as weather data available hourly
                                nearest_hour_schedule_df=schedule_df.copy()
                                nearest_hour_schedule_df['estimated_arrival_nearest_hour']=nearest_hour_schedule_df['estimated_arrival'].dt.round("H")
                                nearest_hour_schedule_df['departure_date_nearest_hour']=nearest_hour_schedule_df['departure_date'].dt.round("H")
                                nearest_hour_schedule_route_df=pd.merge(nearest_hour_schedule_df, routes_df, on='route_id', how='left')
          * Weather Data
         
                            * reate a copy of the 'weather_df' DataFrame for manipulation
                            * Drop the 'date' and 'hour' columns from 'origin_weather_data'
                            * Create a copy of the 'weather_df' DataFrame for manipulation
                            * Drop the 'date' and 'hour' columns from 'destination_weather_data'
                            * Merge 'nearest_hour_schedule_route_df' with 'origin_weather_data' based on specified columns
                            * Merge 'origin_weather_merge' with 'destination_weather_data' based on specified columns
            
         *  Traffic and Schedule  Data Merge

             * Create a copy of the schedule DataFrame for manipulation
             * Round 'estimated_arrival' times to the nearest hour
             * Round 'departure_date' times to the nearest hour

               hourly_exploded_scheduled_df=(nearest_hour_schedule_df.assign(custom_date = [pd.date_range(start, end, freq='H')  # Create custom date ranges
                          for start, end
                          in zip(nearest_hour_schedule_df['departure_date'], nearest_hour_schedule_df['estimated_arrival'])])  # Using departure and estimated arrival times
                          .explode('custom_date', ignore_index = True))  # Explode the DataFrame based on the custom date range
          
      
                           scheduled_traffic=hourly_exploded_scheduled_df.merge(traffic_df,on=['route_id','custom_date'],how='left')

                * Define a custom aggregation function for accidents
     
                              def custom_agg(values):
                                  """
                                  Custom aggregation function to determine if any value in a group is 1 (indicating an accident).
                              
                                  Args:
                                  values (iterable): Iterable of values in a group.
                              
                                  Returns:
                                  int: 1 if any value is 1, else 0.
                                  """
                                  if any(values == 1):
                                      return 1
                                  else:
                                      return 0
                              
                   * Group by 'unique_id', 'truck_id', and 'route_id', and apply custom aggregation
                                scheduled_route_traffic = scheduled_traffic.groupby(['unique_id', 'truck_id', 'route_id'], as_index=False).agg(
                                    avg_no_of_vehicles=('no_of_vehicles', 'mean'),
                                    accident=('accident', custom_agg)
                                )
      - Merge Data
      
        * Merge schedule_route_traffic with origin_destination_weather
  
            origin_destination_weather_traffic_merge=origin_destination_weather.merge(scheduled_route_traffic,on=['unique_id','truck_id','route_id'],how='left')
            
        * merge weather &traffic
  
                merged_data_weather_traffic=pd.merge(schedule_weather_merge, origin_destination_weather_traffic_merge, on=['unique_id', 'truck_id', 'route_id', 'departure_date',
                 'estimated_arrival', 'delay'], how='left')
  
          * merge weather traffic trucks   
  
              pd.merge(merged_data_weather_traffic, trucks_df, on='truck_id', how='left')
      
               * Merge merged_data with truck_data based on 'truck_id' column (Left Join)

                     final_merge = pd.merge(merged_data_weather_traffic_trucks, drivers_df, left_on='truck_id', right_on = 'vehicle_no', how='left')

              * Function to check if there is nighttime involved between arrival and departure time

                     def has_midnight(start, end):
                           return int(start.date() != end.date())
                  

               * Apply the function to create a new column indicating nighttime involvement

                         final_merge['is_midnight'] = final_merge.apply(lambda row: has_midnight(row['departure_date'], row['estimated_arrival']), axis=1)

    - Final Data frame:

                           final_feature_descriptions = [
                        {"name": 'unique_id', "description": "the unique identifier for each record"},
                        {"name": 'truck_id', "description": "the unique identifier of the truck"},
                        {"name": 'route_id', "description": "the unique identifier of the route"},
                        {"name": 'departure_date', "description": "departure DateTime of the truck"},
                        {"name": 'estimated_arrival', "description": "estimated arrival DateTime of the truck"},
                        {"name": 'delay', "description": "binary variable if the truck’s arrival was delayed, 0 for on-time arrival and 1 for delayed arrival"},
                        {"name": 'route_avg_temp', "description":  'Average temperature in Fahrenheit'},
                        {"name": 'route_avg_wind_speed', "description":  'Average wind speed in miles per hour'},
                        {"name": 'route_avg_precip', "description":  'Average precipitation in inches'},
                        {"name": 'route_avg_humidity', "description":  'Average humidity observed'},
                        {"name": 'route_avg_visibility', "description":  'Average visibility observed in miles per hour'},
                        {"name": 'route_avg_pressure', "description":  'Average pressure observed in millibar'},
                        {"name": 'route_description', "description":  'description of the weather conditions such as Clear, Cloudy, etc'},
                        {"name": 'estimated_arrival_nearest_hour', "description":  'estimated arrival DateTime of the truck'},
                        {"name": 'departure_date_nearest_hour', "description":  'departure DateTime of the truck'},
                        {"name": 'origin_id', "description": "the city identification number for the origin city"},
                        {"name": 'destination_id', "description": " the city identification number for the destination"},
                        {"name": 'distance', "description": " the distance between the origin and destination cities in miles"},
                        {"name": 'average_hours', "description": "average time needed to travel from the origin to the destination in hours"},
                        {"name": 'origin_temp', "description":  'temperature in Fahrenheit'},
                        {"name": 'origin_wind_speed', "description":  'wind speed in miles per hour'},
                        {"name": 'origin_description', "description":  'description of the weather conditions such as Clear, Cloudy, etc'},
                        {"name": 'origin_precip', "description":  'precipitation in inches'},
                        {"name": 'origin_humidity', "description":  'humidity observed'},
                        {"name": 'origin_visibility', "description":  'visibility observed in miles per hour'},
                        {"name": 'origin_pressure', "description":  'pressure observed in millibar'},
                        {"name": 'destination_temp', "description":  'temperature in Fahrenheit'},
                        {"name": 'destination_wind_speed', "description":  'wind speed in miles per hour'},
                        {"name": 'destination_description', "description":  'description of the weather conditions such as Clear, Cloudy, etc'},
                        {"name": 'destination_precip', "description":  'precipitation in inches'},
                        {"name": 'destination_humidity', "description":  'humidity observed'},
                        {"name": 'destination_visibility', "description":  'visibility observed in miles per hour'},
                        {"name": 'destination_pressure', "description":  'pressure observed in millibar'},
                        {"name": 'avg_no_of_vehicles', "description": "the average number of vehicles observed on the route"},
                        {"name": 'accident', "description": "binary variable to denote if an accident was observed"},
                        {"name":'truck_age',"description":"age of the truck in years"},
                        {"name":'load_capacity_pounds',"description":"loading capacity of the truck in years"},
                        {"name":'mileage_mpg',"description": "mileage of the truck in miles per gallon"},
                        {"name":'fuel_type',"description":"fuel type of the truck"},
                        {"name": "driver_id", "description": "unique identification for each driver"},
                        {"name": "name", "description": " name of the truck driver"},
                        {"name": "gender", "description": "gender of the truck driver"},
                        {"name": "age", "description": "age of the truck driver"},
                        {"name": "experience", "description": " experience of the truck driver in years"},
                        {"name": "driving_style", "description": "driving style of the truck driver, conservative or proactive"},
                        {"name": "ratings", "description": "average rating of the truck driver on a scale of 1 to 5"},
                        {"name": "vehicle_no", "description": "the number of the driver’s truck"},
                        {"name": "average_speed_mph", "description": "average speed the truck driver in miles per hour"},
                        {"name": 'is_midnight', "description": "binary variable to denote if it was midnight"}
                    
                    ]
