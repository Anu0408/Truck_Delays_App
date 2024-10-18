import requests
import pandas as pd
import psycopg2
from psycopg2 import extras
from bs4 import BeautifulSoup

class DataIngestion:
    def __init__(self, config):
        self.db_host = config.get('database', 'host')
        self.db_port = config.get('database', 'port')
        self.db_name = config.get('database', 'name')
        self.db_user = config.get('database', 'user')
        self.db_password = config.get('database', 'password')
        self.github_dir_url = config.get('github', 'dir_url')

    def fetch_csv_file_urls(self, github_dir_url):
        response = requests.get(github_dir_url)
        if response.status_code != 200:
            print(f"Failed to fetch URL: {github_dir_url} with status code {response.status_code}")
            return set()

        soup = BeautifulSoup(response.text, 'html.parser')

        return {
            'https://raw.githubusercontent.com' + link['href'].replace('/blob', '')
            for link in soup.find_all('a', href=True)
            if '/blob/' in link['href'] and link['href'].endswith('.csv')
        }

    def get_column_types(self, df):
        """Generate PostgreSQL column types based on DataFrame dtypes."""
        type_mapping = {
            'object': 'TEXT',
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP'
        }
        return ', '.join(f"{col} {type_mapping.get(str(dtype), 'TEXT')}" for col, dtype in df.dtypes.items())

    def create_table(self, df, table_name, cur):
        columns = self.get_column_types(df)
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {columns});"
        cur.execute(create_table_query)

    def empty_table(self, table_name, cur):
        """Empty the specified table in the database."""
        delete_query = f"DELETE FROM {table_name};"
        cur.execute(delete_query)

    def upsert_data(self, df, table_name, cur):
        cols = ', '.join(df.columns)
        insert_query = f"""
        INSERT INTO {table_name} ({cols})
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns])};
        """
        data = [tuple(row) for row in df.itertuples(index=False, name=None)]
        # Insert data into the database
        extras.execute_values(cur, insert_query, data)
        
        # Return the number of rows inserted
        return len(data)

    def start_ingestion(self):
        # Main logic to fetch, process, and insert data
        with psycopg2.connect(dbname=self.db_name, user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port) as conn:
            with conn.cursor() as cur:
                file_urls = self.fetch_csv_file_urls(self.github_dir_url)
                print(f"Found CSV files: {file_urls}")

                for file_url in file_urls:
                    print(f"Processing {file_url} ...")

                    response = requests.get(file_url)
                    if response.status_code == 200:
                        df = pd.read_csv(file_url)
                        table_name = file_url.split('/')[-1].split('.')[0]

                        self.create_table(df, table_name, cur)
                        print(f"Ensured table {table_name} exists.")

                        # Empty the table before upserting new data
                        self.empty_table(table_name, cur)
                        print(f"Emptied table {table_name}.")

                        # Get the shape of the DataFrame
                        df_shape = df.shape
                        print(f"DataFrame shape before upsert into {table_name}: {df_shape} (Rows: {df_shape[0]}, Columns: {df_shape[1]})")

                        # Upsert data into the database and get the number of rows inserted
                        num_rows_inserted = self.upsert_data(df, table_name, cur)
                        print(f"Upserted {num_rows_inserted} rows into table {table_name}.")

                        # Print final data shape in the database
                        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                        final_count = cur.fetchone()[0]
                        print(f"Final data shape in table {table_name}: {final_count} rows.")

                    else:
                        print(f"Failed to download {file_url}")

                conn.commit()
                print("✅ Data ingestion completed successfully!")

# Sample usage
# config = configparser.RawConfigParser()
# config.read('path_to_config_file')
# ingestion = DataIngestion(config)
# ingestion.start_ingestion()
