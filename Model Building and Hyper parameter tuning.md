2. Truck Delay - Machine Learning Modeling
In Part 2, we delve deeper into the machine-learning pipeline. Focusing on data retrieval from the feature store, train-validation-test split, one-hot encoding, scaling numerical features, and leveraging MLFlow for model experimentation, we will build our pipeline for model building with logistic regression, random forest, and XGBoost models. Further, we explore hyperparameter tuning , discuss grid and random search, and, ultimately, the deployment of a Streamlit application on AWS.

1. Retrieve Truckdealy feature dataset from Hopsworks

2. Verify Dataset

       class 'pandas.core.frame.DataFrame'>
      RangeIndex: 12308 entries, 0 to 12307
      Data columns (total 49 columns):
      
                             #   Column                          Non-Null Count  Dtype         
                  ---  ------                          --------------  -----         
                   0   unique_id                       12308 non-null  int64         
                   1   truck_id                        12308 non-null  int64         
                   2   route_id                        12308 non-null  object        
                   3   departure_date                  12308 non-null  datetime64[ns]
                   4   estimated_arrival               12308 non-null  datetime64[ns]
                   5   delay                           12308 non-null  int64         
                   6   route_avg_temp                  12308 non-null  float64       
                   7   route_avg_wind_speed            12308 non-null  float64       
                   8   route_avg_precip                12308 non-null  float64       
                   9   route_avg_humidity              12308 non-null  float64       
                   10  route_avg_visibility            12308 non-null  float64       
                   11  route_avg_pressure              12308 non-null  float64       
                   12  route_description               12308 non-null  object        
                   13  estimated_arrival_nearest_hour  12308 non-null  datetime64[ns]
                   14  departure_date_nearest_hour     12308 non-null  datetime64[ns]
                   15  origin_id                       12308 non-null  object        
                   16  destination_id                  12308 non-null  object        
                   17  distance                        12308 non-null  float64       
                   18  average_hours                   12308 non-null  float64       
                   19  origin_temp                     12304 non-null  float64       
                   20  origin_wind_speed               12304 non-null  float64       
                   21  origin_description              12308 non-null  object        
                   22  origin_precip                   12304 non-null  float64       
                   23  origin_humidity                 12304 non-null  float64       
                   24  origin_visibility               12304 non-null  float64       
                   25  origin_pressure                 12304 non-null  float64       
                   26  destination_temp                12308 non-null  float64       
                   27  destination_wind_speed          12308 non-null  float64       
                   28  destination_description         12308 non-null  object        
                   29  destination_precip              12308 non-null  float64       
                   30  destination_humidity            12308 non-null  int64         
                   31  destination_visibility          12308 non-null  float64       
                   32  destination_pressure            12308 non-null  float64       
                   33  avg_no_of_vehicles              12308 non-null  float64       
                   34  accident                        12308 non-null  int64         
                   35  truck_age                       12308 non-null  int64         
                   36  load_capacity_pounds            11704 non-null  float64       
                   37  mileage_mpg                     12308 non-null  int64         
                   38  fuel_type                       12308 non-null  object        
                   39  driver_id                       12308 non-null  object        
                   40  name                            12308 non-null  object        
                   41  gender                          12308 non-null  object        
                   42  age                             12308 non-null  int64         
                   43  experience                      12308 non-null  int64         
                   44  driving_style                   12308 non-null  object        
                   45  ratings                         12308 non-null  int64         
                   46  vehicle_no                      12308 non-null  int64         
                   47  average_speed_mph               12308 non-null  float64       
                   48  is_midnight                     12308 non-null  int64         
                  dtypes: datetime64[ns](4), float64(22), int64(12), object(11)
                  memory usage: 4.6+ MB
3.Check for null values if any and treat them

4.Train-Validation-Test Split

       The data points are divided into two or three datasets, train and test, in a train test split method and train validation test split in three way split. The train data is used to train the model, and the model 
        is then used to predict on the test data to see how the model performs on unseen data and whether it is overfitting or underfitting.


       The validation set is a different set of data from the training set that is used to validate the performance of our model during training. This validation approach gives data that allows us to fine-tune the 
        model's hyperparameters and configurations.
        
       Once model optimization is done with the help of the validation set, the model is then used to test unseen data.                                                        


       **** selecting necessary columns and removing id columns ****
       
       cts_cols=['route_avg_temp', 'route_avg_wind_speed',
              'route_avg_precip', 'route_avg_humidity', 'route_avg_visibility',
              'route_avg_pressure', 'distance', 'average_hours',
              'origin_temp', 'origin_wind_speed', 'origin_precip', 'origin_humidity',
              'origin_visibility', 'origin_pressure',
              'destination_temp','destination_wind_speed','destination_precip',
              'destination_humidity', 'destination_visibility','destination_pressure',
               'avg_no_of_vehicles', 'truck_age','load_capacity_pounds', 'mileage_mpg',
               'age', 'experience','average_speed_mph']
       
       
       cat_cols=['route_description',
              'origin_description', 'destination_description',
               'accident', 'fuel_type',
              'gender', 'driving_style', 'ratings','is_midnight']
       
       
       target=['delay']
Checking the date range

final_merge['estimated_arrival'].min(), final_merge['estimated_arrival'].max()

Splitting the data into training, validation, and test sets based on date

       train_df = final_merge[final_merge['estimated_arrival'] <= pd.to_datetime('2019-01-30')]
       
       validation_df = final_merge[(final_merge['estimated_arrival'] > pd.to_datetime('2019-01-30')) &
       
                                   (final_merge['estimated_arrival'] <= pd.to_datetime('2019-02-07'))]
       
       test_df = final_merge[final_merge['estimated_arrival'] > pd.to_datetime('2019-02-07')]

      
       
       X_train=train_df[cts_cols+cat_cols]

       y_train=train_df['delay']


       X_valid = validation_df[cts_cols + cat_cols]

       y_valid = validation_df['delay']

       
                  
       X_test=test_df[cts_cols+cat_cols]
                  
       y_test=test_df['delay']
Encoding
   - columns to be encoded (OneHotEncoder) : 
   
       ecoder_columns = ['route_description', 'origin_description', 'destination_description', 'fuel_type', 'gender', 'driving_style']

   - Generating names for the new one-hot encoded features

       encoded_features = list(encoder.get_feature_names_out(encode_columns))

   - Transforming the training, validation, and test sets

      X_train[encoded_features] = encoder.transform(X_train[encode_columns])

      X_valid[encoded_features] = encoder.transform(X_valid[encode_columns])

      X_test[encoded_features] = encoder.transform(X_test[encode_columns])
   
  - Dropping the original categorical features

    X_train = X_train.drop(encode_columns, axis=1)

    X_valid = X_valid.drop(encode_columns, axis=1)

    X_test = X_test.drop(encode_columns, axis=1)
Scaling Numerical Features
  - Create standard scaler
  - transform all categorical columns with standard scaler for each of X_train, X_valid, X_test data sets
Build model with MLFLOW, GRIDSearch, Hyperperameter tunning AND Evaluate Model performances
- Logistic Regression
- Random Forest
- XGBoost
