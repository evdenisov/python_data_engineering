from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import glob
import json
import numpy as np

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2026, 1, 1),
}

def load_parquet():
    folder = '/opt/airflow/dags/data/parquet'
    files = glob.glob(os.path.join(folder, '*.parquet'))
    
    if not files:
        print(f"⚠️ Нет файлов в {folder}")
        return
    
    # Ограничиваем количество файлов - берем только первые 3
    files = sorted(files)[:1]  # сортируем и берем первые 3
    print(f"📂 Для загрузки выбрано файлов: {len(files)} из {len(glob.glob(os.path.join(folder, '*.parquet')))}")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

     # Очищаем таблицу перед загрузкой
    cursor.execute("TRUNCATE TABLE hse_python_etl_project.raw_parquet_data;")
    conn.commit()
    print("🗑️ Таблица очищена")
    
    total_rows = 0
    
    for file in files:
        name = os.path.basename(file)
        df = pd.read_parquet(file)
        
        # Заменяем NaN на None во всем DataFrame
        df = df.replace({np.nan: None})
        
        for _, row in df.iterrows():
            # Преобразуем все значения
            row_dict = {}
            for k, v in row.to_dict().items():
                if isinstance(v, (pd.Timestamp, datetime)):
                    row_dict[k] = str(v)
                else:
                    row_dict[k] = v
            
            cursor.execute(
                "INSERT INTO hse_python_etl_project.raw_parquet_data (file_name, row_data) VALUES (%s, %s)",
                (name, json.dumps(row_dict, default=str))
            )
        
        conn.commit()
        print(f"✅ {name}: {len(df)} строк")
    
    cursor.close()
    conn.close()

dag = DAG(
    'PDE_1_extract_parquet_data',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
)

load_task = PythonOperator(
    task_id='load_parquet_data',
    python_callable=load_parquet,
    dag=dag,
)