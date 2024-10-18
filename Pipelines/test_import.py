# Pipelines/test_import.py
import sys
import os

# Add src directory to system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.components.Data_Cleaning import DataCleaning
    print("Import successful!")
except ModuleNotFoundError as e:
    print("Import failed:", e)
