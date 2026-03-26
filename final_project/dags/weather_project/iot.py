import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


def process_data():
    import pandas as pd
    df = pd.read_csv('dags/data/weather_project/IOT-temp.csv')
    df['date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M').dt.date
    df_new = df[df['out/in'] == 'In']
    df_new['percentile_rank'] = df_new['temp'].rank(pct=True) * 100
    df_newest = df_new[(df_new['percentile_rank'] > 5) & (df_new['percentile_rank']< 95)]
    df_newest_final = df_newest.groupby('date')['temp'].agg(('max', 'min')).reset_index()   
    print("Самые теплые дни", df_newest_final.sort_values(by='max', ascending=False)['date'].head())
    print("Самые холодные дни", df_newest_final.sort_values(by='min', ascending=True)['date'].head())

with DAG(
    dag_id='iot_temp_simple',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    process_task = PythonOperator(
        task_id='process_iot_data',
        python_callable=process_data
    )
  
    process_task


# df = pd.read_csv('IOT-temp.csv')
# df['date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M').dt.date
# df_new = df[df['out/in'] == 'In']
# df_new['percentile_rank'] = df_new['temp'].rank(pct=True) * 100
# df_newest = df_new[(df_new['percentile_rank'] > 5) & (df_new['percentile_rank']< 95)]
# df_newest_final = df_newest.groupby('date')['temp'].agg(('max', 'min')).reset_index()   
# print("Самые теплые дни", df_newest_final.sort_values(by = 'max', ascending = False)['date'].head())
# print("Самые холодные дни", df_newest_final.sort_values(by = 'min', ascending = True)['date'].head())