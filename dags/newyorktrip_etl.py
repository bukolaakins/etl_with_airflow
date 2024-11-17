
import pandas as pd
import clickhouse_connect
import sqlite3
import json

# load clickhouse connection string
with open('/opt/airflow/config/conn_string.json') as cxn:
    config = json.load(cxn)

#create connection to clickhouse

def connect_to_clickhousedb():
    try:
        client = clickhouse_connect.get_client(
            host=config['host'],
            port=config['port'],
            username=config['username'],
            password=config['password']
        )
        print("Connection successful!")
        return client
    except Exception as e:
        print(f"Connection failed: {e}")
        return None


# extract metrics from clickhouse

def extract_db_metrics(client):
    if client is None:
        print("No client connection available.")
        return None

    query = '''
        WITH cteTripSummary AS (
            SELECT  
                toDayOfWeek(pickup_datetime) AS day,
                formatDateTime(pickup_datetime, '%m-%Y') AS mmyy,
                AVG(fare_amount) AS avg_fare,
                COUNT(*) AS total_trips,
                AVG(dateDiff('second', pickup_datetime, dropoff_datetime)) AS trip_duration_s
            FROM 
                tripdata
            WHERE 
                pickup_datetime BETWEEN '2014-01-01' AND '2016-12-31'
            GROUP BY 
                day, mmyy
            ORDER BY 
                mmyy, day
        )

        SELECT
            mmyy month, 
            avgIf(total_trips, day = 6) sat_mean_trip_count,
            avgIf(avg_fare, day = 6) sat_mean_fare_per_trip,
            avgIf(trip_duration_s, day = 6) sat_mean_duration_per_trip,
            avgIf(total_trips, day = 7) sun_mean_trip_count,
            avgIf(avg_fare, day = 7) sun_mean_fare_per_trip,
            avgIf(trip_duration_s, day = 7) sun_mean_duration_per_trip
        FROM
            cteTripSummary
        GROUP BY
            month
        ORDER BY
            month
    '''

    try:
        query_result = client.query(query)
        df = pd.DataFrame(query_result.result_rows, columns=query_result.column_names)
        return df
    except Exception as e:
        print(f"Query execution failed: {e}")
        return None


# load data to sqlite db
def load_to_db(df, db_path):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)

    # Insert data into SQLite
    df.to_sql('monthlymetrics', conn, if_exists='replace', index=False)

    # Commit and close the connection
    conn.commit()
    conn.close()

# Connect to clickhouse
client = connect_to_clickhousedb()

# Extract data from clickhouse and load to sqlite db 
if client:
    df = extract_db_metrics(client)
    if df is not None:
        # Load data to SQLite and check connection
        load_to_db(df, '/opt/airflow/newyorktaxi.db')
        print("data loaded successfully")
    else:
        print("No data was returned from the query.")
else:
    print("Failed to connect to ClickHouse.")
