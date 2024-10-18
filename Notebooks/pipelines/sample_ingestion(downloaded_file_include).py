import requests
import pandas as pd
import psycopg2
import os
from bs4 import BeautifulSoup

# URL of the GitHub repository directory (HTML page)
GITHUB_DIR_URL = "https://github.com/sekhar4ml/MLAppsData/tree/main/truck_delay"

# Directory to save downloaded CSV files
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Database connection parameters
db_host = 'localhost'
db_port = '5432'
db_name = 'truck_delay_classification'
db_user = 'postgres'
db_password = 'Anu0408'

# Function to fetch CSV file URLs from the GitHub directory page
def fetch_csv_file_urls():
    response = requests.get(GITHUB_DIR_URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    file_urls = set()  # Use a set to avoid duplicates
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('.csv'):
            file_url = 'https://raw.githubusercontent.com' + href.replace('/blob', '')
            file_urls.add(file_url)  # Add to set (automatically handles duplicates)
    
    return file_urls

# Function to download a file
def download_file(url, local_path):
    response = requests.get(url)
    with open(local_path, 'wb') as file:
        file.write(response.content)

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cur = conn.cursor()

# Function to create table (if not exists) and insert data into PostgreSQL
def create_table_and_insert_data(df, table_name):
    # Drop table if it exists
    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    cur.execute(drop_table_query)
    
    # Create table
    create_table_query = f"""
    CREATE TABLE {table_name} (
        id SERIAL PRIMARY KEY,
        {', '.join([f'{col} TEXT' for col in df.columns])}
    );
    """
    cur.execute(create_table_query)

    # Insert DataFrame into PostgreSQL database
    for index, row in df.iterrows():
        insert_query = f"""
        INSERT INTO {table_name} ({', '.join(df.columns)})
        VALUES ({', '.join(['%s' for _ in df.columns])});
        """
        cur.execute(insert_query, tuple(row))

# Fetch CSV file URLs from the GitHub directory
file_urls = fetch_csv_file_urls()

# Track downloaded files to avoid duplicates
downloaded_files = set()

# Download CSV files and insert data into the local database
for file_url in file_urls:
    file_name = file_url.split('/')[-1]
    local_path = os.path.join(DATA_DIR, file_name)
    
    # Download the file only if it hasn't been downloaded yet
    if file_name not in downloaded_files:
        download_file(file_url, local_path)
        downloaded_files.add(file_name)
        print(f"Downloaded {file_name} to {local_path}")
    
    # Read CSV file and insert into database
    df = pd.read_csv(local_path)
    
    # Create a table name based on the file name (remove extension)
    table_name = os.path.splitext(file_name)[0]
    
    create_table_and_insert_data(df, table_name)
    print(f"Inserted data from {file_name} into table {table_name}")

# Commit the transaction
conn.commit()

# Close the cursor and database connection
cur.close()
conn.close()
print("✅ Done!")
