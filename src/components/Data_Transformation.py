import os
import pandas as pd
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler

class DataTransformation:
    def __init__(self, output_path):
        # The path to save the output CSV
        self.output_path = output_path

        # Create the output directory if it does not exist
        os.makedirs(self.output_path, exist_ok=True)

        # Hardcoded columns for transformation
        self.categorical_columns = [
            'truck_id', 'route_id', 'route_description', 
            'origin_description', 'destination_description', 'fuel_type', 
            'driving_style', 'gender'
        ]
        self.numerical_columns = [
            'route_avg_temp', 'route_avg_wind_speed', 'route_avg_precip', 
            'route_avg_humidity', 'route_avg_visibility', 'route_avg_pressure', 
            'distance', 'average_hours', 'origin_temp', 'origin_wind_speed', 
            'origin_precip', 'origin_humidity', 'origin_visibility', 
            'origin_pressure', 'destination_temp', 'destination_wind_speed', 
            'destination_precip', 'destination_humidity', 'destination_visibility', 
            'destination_pressure', 'avg_no_of_vehicles', 'truck_age', 
            'load_capacity_pounds', 'mileage_mpg', 'age', 'experience', 
            'average_speed_mph'
        ]
        self.target_column = 'delay'

    def handle_missing_values(self, df):
        """Handles missing values in the DataFrame."""
        print("🧹 Handling missing values...")

        # Fill missing numerical values with the median and categorical with mode
        for col in self.numerical_columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].median())
        
        for col in self.categorical_columns:
            if col in df.columns:
                # Check if mode is not empty before using it
                if df[col].mode().size > 0:
                    df[col] = df[col].fillna(df[col].mode()[0])

        print("✅ Missing values handled.")
        return df

    def convert_dates(self, df):
        """Convert 'estimated_arrival' to datetime format."""
        print("🔄 Converting 'estimated_arrival' to datetime...")
        if 'estimated_arrival' in df.columns:
            df['estimated_arrival'] = pd.to_datetime(df['estimated_arrival'], errors='coerce')
            print("✅ Conversion complete.")
        else:
            print("⚠️ 'estimated_arrival' column not found.")
        return df

    def split_data_by_date(self, df):
        """Split data into training, validation, and test sets based on date."""
        print("🔄 Splitting data into train, validation, and test sets by date...")

        # Split based on 'estimated_arrival' date
        train_df = df[df['estimated_arrival'] <= pd.to_datetime('2019-01-30')]
        validation_df = df[(df['estimated_arrival'] > pd.to_datetime('2019-01-30')) & 
                           (df['estimated_arrival'] <= pd.to_datetime('2019-02-07'))]
        test_df = df[df['estimated_arrival'] > pd.to_datetime('2019-02-07')]

        print(f"✅ Train set shape: {train_df.shape}")
        print(f"✅ Validation set shape: {validation_df.shape}")
        print(f"✅ Test set shape: {test_df.shape}")

        return train_df, validation_df, test_df

    def encode_categorical_variables(self, df):
        """Perform label encoding for categorical variables."""
        label_encoder = LabelEncoder()
        for col in self.categorical_columns:
            if col in df.columns:
                df[col] = label_encoder.fit_transform(df[col].astype(str))
            else:
                print(f"Column {col} not found in DataFrame for encoding")
        return df

    def onehot_encode(self, df):
        """Perform one-hot encoding for specified columns."""
        onehot_encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
        encoded_features = onehot_encoder.fit_transform(df[self.categorical_columns])
        encoded_feature_names = onehot_encoder.get_feature_names_out(self.categorical_columns)
        df_encoded = pd.DataFrame(encoded_features, columns=encoded_feature_names, index=df.index)
        df = pd.concat([df, df_encoded], axis=1)
        df = df.drop(self.categorical_columns, axis=1)  # Drop original categorical columns
        return df

    def scale_numerical_features(self, df):
        """Scale numerical features using StandardScaler."""
        scaler = StandardScaler()
        df[self.numerical_columns] = scaler.fit_transform(df[self.numerical_columns])
        return df

    def transform_data(self, df):
        """Main transformation function for the data."""
        print("🛠 Starting data transformation process...")

        # Convert date column
        df = self.convert_dates(df)

        # Handle missing values (if applicable)
        df = self.handle_missing_values(df)

        # Split data into train, validation, and test sets by date
        train_df, validation_df, test_df = self.split_data_by_date(df)

        # Separate features and target
        X_train, y_train = train_df.drop(self.target_column, axis=1), train_df[self.target_column]
        X_valid, y_valid = validation_df.drop(self.target_column, axis=1), validation_df[self.target_column]
        X_test, y_test = test_df.drop(self.target_column, axis=1), test_df[self.target_column]

        # Process each dataset
        for subset, name in zip([X_train, X_valid, X_test], ['train_data', 'validation_data', 'test_data']):
            # Encode categorical variables
            subset = self.encode_categorical_variables(subset)

            # One-hot encode specific columns
            subset = self.onehot_encode(subset)

            # Scale numerical columns
            subset = self.scale_numerical_features(subset)

            # Save the transformed subset to CSV
            self.save_to_csv(subset, name)

        print("✅ Data transformation complete!")
        return (X_train, y_train), (X_valid, y_valid), (X_test, y_test)

    def save_to_csv(self, df, name):
        """Save the transformed data to CSV."""
        output_file = os.path.join(self.output_path, f"{name}.csv")
        
        # Remove existing file if it exists
        if os.path.exists(output_file):
            os.remove(output_file)
        
        # Save the DataFrame to CSV
        df.to_csv(output_file, index=False)
        print(f"✅ Transformed data saved to {output_file}")

# Example usage:
# output_path = "C:/Users/anucv/OneDrive/Desktop/AI and ML training/Machine_Learning/TRUCK_DELAY_CLASSIFICATION_PROJECT/Data/Output/transformed_data"
# transformer = DataTransformation(output_path)
# df_cleaned = pd.read_csv('your_cleaned_data.csv')  # Make sure to replace this with your actual cleaned data path
# transformer.transform_data(df_cleaned)
