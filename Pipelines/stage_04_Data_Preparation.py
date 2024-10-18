import os
import sys
import configparser
import pandas as pd
from src.components.Data_Preparation import DataPreparationComponent

# Add src directory to system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load configuration
config = configparser.ConfigParser()
config.read('C:/Users/anucv/OneDrive/Desktop/AI and ML training/Machine_Learning/TRUCK_DELAY_CLASSIFICATION_PROJECT/Config/config.ini')

# Path to store cleaned data
cleaned_data_path = config.get('PATHS', 'cleaned_data_path')

STAGE_NAME = "Data Preparation"

class DataPreparationPipeline:
    def __init__(self):
        self.component = DataPreparationComponent()

    def run_preparation_pipeline(self):
        try:
            print(">>> Starting data preparation stage <<<")
            
            # Prepare the data using the DataPreparationComponent
            final_dataset = self.component.prepare_data()

            if final_dataset is not None:  # Check if final_dataset is not None
                # Define output path for prepared data
                output_path = os.path.join(cleaned_data_path, 'final_prepared_data.csv')
                final_dataset.to_csv(output_path, index=False)
                print(f"Prepared data saved to: {output_path}")
            else:
                print("Data preparation failed. Final dataset is None.")

            print(">>> Data preparation stage completed <<<\n")

        except Exception as e:
            print(f"Error during data preparation: {e}")

if __name__ == '__main__':
    try:
        print(">>>>>> Stage started <<<<<< :", STAGE_NAME)
        pipeline = DataPreparationPipeline()
        pipeline.run_preparation_pipeline()
        print(">>>>>> Stage completed <<<<<<", STAGE_NAME)
    except Exception as e:
        print(f"Error in Data Preparation Pipeline: {e}")
