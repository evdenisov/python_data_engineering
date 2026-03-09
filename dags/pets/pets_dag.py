from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json
filename = 'dags/data/pets-data.json'
def json_to_postgres():
    with open(filename, 'r') as f:
        data = json.load(f)
    df = pd.json_normalize(data, 'pets')
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    df.to_sql('pets', 
              engine, 
              if_exists='replace',
              index=False)
    print('Таблица pets загружена')
with DAG(
    'pets_to_postgres',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once',
    catchup=False,
) as dag:
    
    load_task = PythonOperator(
        task_id='load_json_to_postgres',
        python_callable=json_to_postgres
    )