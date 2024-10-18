import requests
import pandas as pd
import psycopg2
from psycopg2 import extras
from bs4 import BeautifulSoup
from io import StringIO

# URL of the GitHub repository directory
GITHUB_DIR_URL = "https://github.com/sekhar4ml/MLAppsData/tree/main/truck_delay"

# Database connection parameters
db_host = 'localhost'
db_port = '5432'
db_name = 'truck_delay_classification'
db_user = 'postgres'
db_password = 'Anu0408'

def fetch_csv_file_urls(github_dir_url):
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

def get_column_types(df):
    """Generate PostgreSQL column types based on DataFrame dtypes."""
    type_mapping = {
        'object': 'TEXT',
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    return ', '.join(f"{col} {type_mapping.get(str(dtype), 'TEXT')}" for col, dtype in df.dtypes.items())

def create_table(df, table_name, cur):
    columns = get_column_types(df)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {columns});"
    cur.execute(create_table_query)

def upsert_data(df, table_name, cur):
    cols = ', '.join(df.columns)
    insert_query = f"""
    INSERT INTO {table_name} ({cols})
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
    {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns])};
    """
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]
    extras.execute_values(cur, insert_query, data)

def main():
    with psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port) as conn:
        with conn.cursor() as cur:
            file_urls = fetch_csv_file_urls(GITHUB_DIR_URL)
            print(f"Found CSV files: {file_urls}")

            for file_url in file_urls:
                print(f"Processing {file_url} ...")
                
                response = requests.get(file_url)
                if response.status_code == 200:
                    # csv_content = response.content.decode('utf-8')
                    df = pd.read_csv(file_url)
                    table_name = file_url.split('/')[-1].split('.')[0]
                    
                    create_table(df, table_name, cur)
                    print(f"Ensured table {table_name} exists.")

                    upsert_data(df, table_name, cur)
                    print(f"Upserted data into table {table_name}.")
                else:
                    print(f"Failed to download {file_url}")

            conn.commit()
            print("✅ Data ingestion completed successfully!")

if __name__ == "__main__":
    main()