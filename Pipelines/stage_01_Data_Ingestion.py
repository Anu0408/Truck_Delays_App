import os
import sys
import configparser

# Add src directory to system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.components.Data_Ingestion import DataIngestion

# Load configuration
config = configparser.RawConfigParser()
config.read('C:/Users/anucv/OneDrive/Desktop/AI and ML training/Machine_Learning/TRUCK_DELAY_CLASSIFICATION_PROJECT/Config/config.ini')

STAGE_NAME = "Data Ingestion"

class DataIngestionPipeline:
    def __init__(self):
        self.ingestion_obj = DataIngestion(config)

    def main(self):
        try:
            self.ingestion_obj.start_ingestion()  # Use start_ingestion instead of process_csv_files
        except Exception as e:
            raise e

if __name__ == '__main__':
    try:
        print(">>>>>> Stage started <<<<<< :", STAGE_NAME)
        obj = DataIngestionPipeline()
        obj.main()
        print(">>>>>> Stage completed <<<<<<", STAGE_NAME)
    except Exception as e:
        print(e)
        raise e
