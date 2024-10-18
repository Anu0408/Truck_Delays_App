import os
import sys
import configparser
import hopsworks

# Add src directory to system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.components.Data_Transformation import DataTransformation

# Load configuration
config = configparser.RawConfigParser()
config.read('C:/Users/anucv/OneDrive/Desktop/AI and ML training/Machine_Learning/TRUCK_DELAY_CLASSIFICATION_PROJECT/Config/config.ini')

STAGE_NAME = "Data Transformation"

class DataTransformationPipeline:
    def __init__(self):
        # Get the API key and paths from config
        api_key = config.get('HopsWorks', 'api_key')
        self.output_path = config.get('HopsWorks', 'output_path')

        # Connect to Hopsworks feature store using API key from config
        self.project = hopsworks.login(api_key_value=api_key)
        self.fs = self.project.get_feature_store()
        self.transformation_obj = DataTransformation(self.output_path)

    def fetch_feature_store_data(self):
        try:
            final_merged_fg = self.fs.get_feature_group(name="final_merged", version=1)
            final_merged_df = final_merged_fg.read()
            print("✅ Fetched data from feature store")
            return final_merged_df
        except Exception as e:
            print("❌ Failed to fetch data from feature store:", e)
            raise e

    def main(self):
        try:
            # Fetch data from Hopsworks feature store
            df_cleaned = self.fetch_feature_store_data()
            
            # Apply transformations
            train_df, validation_df, test_df = self.transformation_obj.transform_data(df_cleaned)
            
            # Save the transformed data (already saved in transform_data method)

        except Exception as e:
            raise e

if __name__ == '__main__':
    try:
        print(">>>>>> Stage started <<<<<< :", STAGE_NAME)
        obj = DataTransformationPipeline()
        obj.main()
        print(">>>>>> Stage completed <<<<<<", STAGE_NAME)
    except Exception as e:
        print(e)
        raise e
