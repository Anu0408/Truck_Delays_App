import pandas as pd

class DataCleaning:
    def __init__(self, df: pd.DataFrame, df_name: str):
        self.df = df.copy()  # Make a copy to preserve the original DataFrame
        self.df_name = df_name

    def identify_nulls(self):
        """Identify and count null values in the DataFrame."""
        null_counts = self.df.isnull().sum()
        print(f"Null values in {self.df_name}:\n{null_counts[null_counts > 0]}\n")
        return null_counts

    def treat_nulls(self):
        """Drop rows with null values from the DataFrame."""
        initial_shape = self.df.shape[0]
        self.df = self.df.dropna()  # Drop rows with null values
        removed_nulls = initial_shape - self.df.shape[0]
        print(f"Removed {removed_nulls} rows with null values in {self.df_name}. Remaining rows: {self.df.shape[0]}")

    def identify_outliers(self, numerical_features):
        """Identify and remove outliers from specified numerical features."""
        outliers = {}
        for feature in numerical_features:
            Q1 = self.df[feature].quantile(0.25)
            Q3 = self.df[feature].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Condition for outliers
            outlier_condition = (self.df[feature] < lower_bound) | (self.df[feature] > upper_bound)
            outliers[feature] = self.df[outlier_condition].shape[0]  # Count outliers
            
            # Remove outliers
            self.df = self.df[~outlier_condition]
            print(f"Removed {outliers[feature]} outliers in {feature} from {self.df_name}.")
        
        return outliers

    def remove_duplicates(self):
        """Remove duplicate rows from the DataFrame."""
        initial_shape = self.df.shape[0]
        self.df = self.df.drop_duplicates()
        removed_duplicates = initial_shape - self.df.shape[0]
        print(f"Removed {removed_duplicates} duplicate rows from {self.df_name}.")

    def convert_date_and_hour(self):
        """Convert date and hour to datetime and create date_time column."""
        if 'date' in self.df.columns and 'hour' in self.df.columns:
            self.df['date'] = pd.to_datetime(self.df['date'])
            # Convert 'hour' to int format (removing leading zeros and converting to 24-hour format)
            self.df['hour'] = (self.df['hour'] // 100).astype(int)
            # Create a new date_time column by combining 'date' and 'hour'
            self.df['date_time'] = self.df['date'] + pd.to_timedelta(self.df['hour'], unit='h')
            print(f"Combined 'date' and 'hour' into 'date_time' for {self.df_name}.")

    def display_cleaned_data(self):
        """Display the cleaned DataFrame."""
        print(f"Cleaned DataFrame ({self.df_name}):")
        print(self.df.head())  # Display the first few rows of the cleaned DataFrame

    def full_cleaning_process(self, numerical_features):
        """Run the full data cleaning process."""
        print("Available columns in DataFrame:", self.df.columns)  # Debugging line
    
        # Identify and treat null values
        self.identify_nulls()
        self.treat_nulls()

        # Identify and treat outliers
        outliers = self.identify_outliers(numerical_features)

        # Convert date and hour to datetime
        self.convert_date_and_hour()

        # Remove duplicates
        self.remove_duplicates()

        # Print the final shape and datatypes of the DataFrame
        print(f"Final shape of {self.df_name}: {self.df.shape}")
        print("Columns after cleaning:", self.df.columns)  # Debugging line
        print("Data types after cleaning:\n", self.df.dtypes)  # Print data types
        
        # Display the cleaned data
        self.display_cleaned_data()

        return self.df
