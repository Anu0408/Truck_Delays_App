import requests
import pandas as pd
import psycopg2
from bs4 import BeautifulSoup

print("Fetching data from GitHub...")

# URL of the GitHub repository directory (HTML page containing CSV files)
GITHUB_DIR_URL = "https://github.com/sekhar4ml/MLAppsData/tree/main/truck_delay"

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
            # Convert GitHub blob link to raw content link
            file_url = 'https://raw.githubusercontent.com' + href.replace('/blob', '')
            file_urls.add(file_url)  # Add to set (automatically handles duplicates)
    
    return file_urls

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

# Process CSV files and insert data into the local database
for file_url in file_urls:
    print(f"Processing {file_url} ...")
    
    # Fetch CSV content directly from GitHub
    response = requests.get(file_url)
    if response.status_code == 200:
        csv_content = response.content.decode('utf-8')
        df = pd.read_csv(pd.compat.StringIO(csv_content))
        
        # Create a table name based on the file name (remove extension)
        table_name = file_url.split('/')[-1].split('.')[0]
        
        # Insert data into PostgreSQL table
        create_table_and_insert_data(df, table_name)
        print(f"Inserted data from {file_url} into table {table_name}")
    else:
        print(f"Failed to download {file_url}")

# Commit the transaction
conn.commit()

# Close the cursor and database connection
cur.close()
conn.close()

print("✅ Data insertion completed successfully!")
