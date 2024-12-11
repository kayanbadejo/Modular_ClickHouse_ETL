from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

from modules.helpers import get_client, get_postgres_engine
from modules.extract import fetch_data, get_last_loaded_date
from modules.load import load_csv_to_postgres, exec_procedure
from dotenv import load_dotenv

load_dotenv(override=True)
client = get_client()
engine = get_postgres_engine()


# Get Max Date
max_date = get_last_loaded_date(engine=engine)


# Query
query = f'''
        select pickup_date, vendor_id, passenger_count, trip_distance, payment_type, fare_amount, tip_amount 
        from tripdata
        where pickup_date = toDate('{max_date}') + 1
        '''

# Setting Default arguments

default_args = {
      'owner' :'kehinde_ayanbadejo',
      'start_date' : datetime(year=2024, month=11, day = 21),
      'email_on_failure' : False,
      'email_on_retry': False,
      'retries': None,
      #'retry_delay' : timedelta(minutes=10)        
}

# Instantiate DAG

with DAG(
    'clickhouse_etl_job', 
    default_args = default_args,
    description  = 'An ETL Pipeline for DivyTrips from ClickHouse to Postgres',
    schedule_interval = '40 11 * * *',
    catchup = False
) as dag:
     # Define Task 1
     start_task = DummyOperator(
       task_id = 'Start_pipeline'
     )
     
     # Define Task 2
     extract_task = PythonOperator(
       task_id = 'extract',
       python_callable = fetch_data,
       op_kwargs = {'client':client, 'query':query}
     )
     
     # Define Task 3
     staging_load_task = PythonOperator(
       task_id = 'staging_load',
       python_callable = load_csv_to_postgres,
       op_kwargs = {'table_name': 'tripdata', 'engine' : engine, 'schema': 'Staging'}
     )
     
     
     # Define Task 4
     exec_procedure_task = PythonOperator(
       task_id = 'exec_prc',
       python_callable = exec_procedure,
       op_kwargs = {'engine' : engine}
     )
      # Define Task 5
     end_task = DummyOperator(
       task_id = 'End_Pipeline'     
     )
     
# Set Dependencies
start_task >> extract_task >> staging_load_task >> exec_procedure_task >> end_task

