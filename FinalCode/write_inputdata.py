import psycopg2
from psycopg2 import pool
import os
import glob

# Database connection parameters
conn_params = {
    'dbname': 'postgres',
    'user': 'caichengyun',
    'password': '123412341234',
    'host': 'localhost',
    'port': 5432
}

# Initialize connection pool
connection_pool = pool.SimpleConnectionPool(1, 20, **conn_params)

# Directory containing the .csv files
input_dir = "./processed_gold_update/" 

csv_files = glob.glob(os.path.join(input_dir, "*.csv"))

# Function to import CSV data into the hypertable
def import_csv_to_db(csv_file, conn):
    with conn.cursor() as cur:
        with open(csv_file, 'r') as f:
            cur.copy_expert(f"COPY target_company_stock_prices(timestamp, company, price) FROM stdin WITH CSV HEADER", f)
        conn.commit()
        print(f"Imported {csv_file} into the database")

# Import each CSV file into the database using connection pool
for csv_file in csv_files:
    conn = connection_pool.getconn()
    try:
        import_csv_to_db(csv_file, conn)
    finally:
        connection_pool.putconn(conn)


connection_pool.closeall()