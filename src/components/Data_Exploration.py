import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class DataExplorationComponent:
    def __init__(self, df_name, df):
        self.df_name = df_name
        self.df = df
        self.numerical_features_dict = {
            'city_weather': ['hour', 'temp', 'wind_speed', 'humidity', 'pressure'],
            'drivers': ['age', 'experience', 'ratings', 'average_speed_mph'],
            'routes': ['distance', 'average_hours'],
            'routes_weather': ['temp', 'wind_speed', 'humidity', 'pressure'],
            'traffic': ['no_of_vehicles', 'accident'],
            'trucks': ['truck_age'],
            'truck_schedule': ['distance', 'estimated_time', 'delay']
        }

    def run(self):
        # Perform basic checks
        self.basic_checks()

        # Convert date columns to datetime if available
        self.convert_date_column()

        # Generate histogram plots for numeric features
        self.plot_histograms()

        # DataFrame-specific analysis
        self.specific_analysis()

    def basic_checks(self):
        # Print basic info and describe
        print(self.df.info())
        print(self.df.describe())
        print(self.df.isnull().sum())  # Check for missing values

    def convert_date_column(self):
        if 'date' in self.df.columns:
            self.df['date'] = pd.to_datetime(self.df['date'])
            print(f"Converted 'date' column to datetime for {self.df_name}")

    def plot_histograms(self):
        numerical_features = self.numerical_features_dict.get(self.df_name, [])
        for feature in numerical_features:
            plt.figure(figsize=(10, 5))
            sns.histplot(self.df[feature], kde=True)
            plt.title(f'{self.df_name}: Histogram of {feature}')
            plt.xlabel(feature)
            plt.ylabel('Frequency')
            plt.show()

    def specific_analysis(self):
        if self.df_name == 'drivers':
            self.explore_drivers()
        elif self.df_name == 'trucks':
            self.explore_trucks()
        elif self.df_name == 'routes':
            self.explore_routes()
        elif self.df_name == 'traffic':
            self.explore_traffic()

    def explore_drivers(self):
        # Scatter plot: Rating vs Average Speed
        plt.figure(figsize=(10, 5))
        sns.scatterplot(x='ratings', y='average_speed_mph', data=self.df)
        plt.title('Drivers: Rating vs Average Speed')
        plt.show()

        # Box plot: Driver Ratings by Gender
        plt.figure(figsize=(10, 5))
        sns.boxplot(x='gender', y='ratings', data=self.df)
        plt.title('Drivers: Ratings by Gender')
        plt.show()

    def explore_trucks(self):
        # Identify low mileage trucks and age distribution
        plt.figure(figsize=(10, 5))
        sns.histplot(self.df['truck_age'], kde=True)
        plt.title('Trucks: Age Distribution')
        plt.xlabel('Truck Age')
        plt.show()

    def explore_routes(self):
        # Histogram plot for numeric values in routes
        self.plot_histograms()

    def explore_traffic(self):
        # Categorize hours of the day into time periods
        def categorize_hour(hour):
            if 0 <= hour < 6:
                return 'Early Morning'
            elif 6 <= hour < 12:
                return 'Morning'
            elif 12 <= hour < 18:
                return 'Afternoon'
            elif 18 <= hour < 24:
                return 'Evening'
            else:
                return 'Unknown'

        self.df['time_period'] = self.df['hour'].apply(categorize_hour)

        # Plot traffic conditions by time period
        plt.figure(figsize=(10, 5))
        sns.countplot(x='time_period', data=self.df)
        plt.title('Traffic: Distribution by Time Period')
        plt.show()
