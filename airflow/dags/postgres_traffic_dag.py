# Importing the necessary modules
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime as dt
from datetime import timedelta
from airflow import DAG 
import pandas as pd

# Specifing the default_args
default_args = {
    'owner': 'biruk',
    'depends_on_past': False,
    'email': ['bkgetmom@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': dt(2022, 7, 18),
    'retry_delay': timedelta(minutes=5)
}

# Reading the data from the csv file
def read_data():
    df = pd.read_csv('/opt/data/sample.csv')
    return df.shape

# Inserting the data into the our postgres table
def insert_data(): 
    pg_hook = PostgresHook(
    postgres_conn_id="postgres_localhost")
    conn = pg_hook.get_sqlalchemy_engine()
    df = pd.read_csv('/opt/data/sample.csv')

    df.to_sql("traffic",
        con=conn,
        if_exists="replace",
        index=False,
    )

# Dag creation
with DAG(
    dag_id='data_to_Postgres_loader_dag',
    default_args=default_args,
    description='Upload data from CSV to MySQL',
    schedule_interval='@once',
    catchup=False
) as pg_dag:
 
 data_reader = PythonOperator(
    task_id="read_data", 
    python_callable=read_data
  )

 table_creator = PostgresOperator(
    task_id="create_table", 
    postgres_conn_id="postgres_localhost",
    sql = '''
            CREATE TABLE  IF NOT EXISTS traffic (
            track_id numeric, 
            type text not null, 
            traveled_d double precision DEFAULT NULL,
            avg_speed double precision DEFAULT NULL, 
            lat double precision DEFAULT NULL, 
            lon double precision DEFAULT NULL, 
            speed double precision DEFAULT NULL,    
            lon_acc double precision DEFAULT NULL, 
            lat_acc double precision DEFAULT NULL, 
            time double precision DEFAULT NULL,
            primary key (track_id)
        );
    '''
    )

 data_loader = PythonOperator(
        task_id="load_data",
        python_callable=insert_data
    )

# Task dependencies
data_reader >> table_creator >> data_loader
