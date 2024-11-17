
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from newyorktrip_etl import connect_to_clickhousedb, extract_db_metrics, load_to_db

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'newyorktrip_etl_dag',
    default_args=default_args,
    description='New York Taxi ETL DAG',
    schedule_interval='0 0 1 * *' #midnight on the first day of every month
)

# run the etl from the python file
def run_etl():
    client = connect_to_clickhousedb()
    
    if client is None:
        raise Exception("Failed to connect to ClickHouse.")    
    df = extract_db_metrics(client)
    
    if df is None:
        raise Exception("No data was returned from the query.")
    
    #load data to sqlite db
    load_to_db(df, '/opt/airflow/newyorktaxi.db')
    print("Data loaded successfully")

run_etl = PythonOperator(
    task_id='complete_newyorktrip_etl',
    python_callable=run_etl,
    dag=dag,
)

