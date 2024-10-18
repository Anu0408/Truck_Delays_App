import pandas as pd

class FeatureStore:
    def __init__(self, config):
        # Initialize connection parameters from config
        self.config = config
        # Assume there is a connection method to initialize the feature store connection

    def get_or_create_feature_group(self, name, version, description, primary_key, event_time, online_enabled):
        # Logic to create or fetch a feature group
        pass

    def insert(self, feature_group, df):
        # Logic to insert DataFrame into the feature group
        pass

    def update_feature_description(self, feature_group, feature_name, description):
        # Logic to update feature description
        pass

    def update_statistics_config(self, feature_group, config):
        # Logic to update statistics configuration
        pass

    def compute_statistics(self, feature_group):
        # Logic to compute statistics for the feature group
        pass
