import os
import sys
import configparser
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
import mlflow
from src.components.Model_Trainer import ModelTrainer

# Add the src directory to system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load configuration
config = configparser.RawConfigParser()
config.read('C:/Users/anucv/OneDrive/Desktop/AI and ML training/Machine_Learning/TRUCK_DELAY_CLASSIFICATION_PROJECT/Config/config.ini')

# Constants from the config file
TRAIN_DATA_PATH = os.path.join(config.get('HopsWorks', 'output_path'), 'train_data.csv')
TEST_DATA_PATH = os.path.join(config.get('HopsWorks', 'output_path'), 'test_data.csv')
VALIDATION_DATA_PATH = os.path.join(config.get('HopsWorks', 'output_path'), 'validation_data.csv')

STAGE_NAME = "MODEL TRAINING WITH MLFLOW, GRIDSEARCH, AND HYPERPARAMETER TUNING"

class MachineLearningModelingPipeline:
    def __init__(self):
        self.model_trainer = ModelTrainer()  # Initialize ModelTrainer component

    def load_data(self):
        # Load the train data
        train_data = pd.read_csv(TRAIN_DATA_PATH)

        # Print data types for debugging
        print("Data Types Before Processing:")
        print(train_data.dtypes)

        # Ensure the 'departure_date' exists in the DataFrame
        if 'departure_date' not in train_data.columns:
            raise KeyError("The expected 'departure_date' is not found in the dataset.")

        # Preprocess the data
        train_data['departure_date'] = pd.to_datetime(train_data['departure_date'], errors='coerce')
        train_data['year'] = train_data['departure_date'].dt.year
        train_data['month'] = train_data['departure_date'].dt.month
        train_data['day'] = train_data['departure_date'].dt.day
        train_data['hour'] = train_data['departure_date'].dt.hour

        # Drop the original date column
        train_data = train_data.drop(columns=['departure_date'])

        # Drop columns with all missing values
        train_data = train_data.dropna(axis=1, how='all')

        # Separate numeric and non-numeric columns
        numeric_data = train_data.select_dtypes(include=['float64', 'int64'])
        non_numeric_data = train_data.select_dtypes(exclude=['float64', 'int64'])

        # Impute missing values for numeric columns only
        numeric_imputer = SimpleImputer(strategy='mean')  # Using mean for numerical columns
        numeric_data_imputed = pd.DataFrame(numeric_imputer.fit_transform(numeric_data), columns=numeric_data.columns)

        # Impute missing values for non-numeric (categorical) columns
        categorical_imputer = SimpleImputer(strategy='most_frequent')  # Using most frequent for categorical columns
        non_numeric_data_imputed = pd.DataFrame(categorical_imputer.fit_transform(non_numeric_data), columns=non_numeric_data.columns)

        # Combine imputed numeric data with non-numeric data
        train_data_imputed = pd.concat([numeric_data_imputed, non_numeric_data_imputed.reset_index(drop=True)], axis=1)

        # Log remaining missing values
        missing_values = train_data_imputed.isnull().sum().sum()
        if missing_values > 0:
            print(f"Warning: There are {missing_values} missing values after imputation. Please check your input data.")

        # Print data types after processing
        print("Data Types After Processing:")
        print(train_data_imputed.dtypes)

        # Check if the dataset is empty after dropping NaNs
        if train_data_imputed.empty:
            raise ValueError("The dataset is empty after processing. Please check your input data.")

        # Encode categorical features
        encoder = OneHotEncoder(sparse_output=False, drop='first')  # Drop the first category to avoid dummy variable trap
        encoded_non_numeric = encoder.fit_transform(non_numeric_data_imputed)
        encoded_columns = encoder.get_feature_names_out(non_numeric_data_imputed.columns)

        # Create DataFrame from encoded data
        encoded_df = pd.DataFrame(encoded_non_numeric, columns=encoded_columns)

        # Combine the encoded categorical data with the imputed numeric data
        final_data = pd.concat([numeric_data_imputed.reset_index(drop=True), encoded_df.reset_index(drop=True)], axis=1)

        # Print final data shape
        print(f"Final data shape: {final_data.shape}")

        # Split features and target
        # Assuming the last column is the target
        X = final_data.iloc[:, :-1]  # Features
        y = final_data.iloc[:, -1]    # Target

        # Split into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        return X_train, X_test, y_train, y_test

    def main(self):
        try:
            print(f">>>>> Stage started <<<<<< : {STAGE_NAME}")
            X_train, X_test, y_train, y_test = self.load_data()
            self.model_trainer.train_and_evaluate("Logistic Regression", X_train, X_test, y_train, y_test)  # Example usage
        except Exception as e:
            print(f"Error occurred during {STAGE_NAME}: {e}")
            raise e

if __name__ == "__main__":
    pipeline = MachineLearningModelingPipeline()
    pipeline.main()
