from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Правильное определение DAG
with DAG(
    'temp_actual',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
    },
    schedule_interval=None,
    catchup=False,
    description='Перенос данных в актуальную таблицу',
) as dag:
    
    def transfer():
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        sql = """
        -- Создаем таблицу если не существует
        CREATE TABLE IF NOT EXISTS temperature_extremes_actual (
            id SERIAL PRIMARY KEY,
            hottest_date DATE,
            coldest_date DATE,
            load_date TIMESTAMP,
            processed_date DATE DEFAULT CURRENT_DATE
        );
        
        -- Вставляем новые данные
        INSERT INTO temperature_extremes_actual 
            (hottest_date, coldest_date, load_date)
        SELECT hottest_date, coldest_date, load_date 
        FROM temperature_extremes
        where load_date::Date >= current_date - 5;
        
        -- Очищаем исходную таблицу (опционально, раскомментируйте если нужно)
        DROP TABLE temperature_extremes;
        """
        
        hook.run(sql)
        print("✅ Данные успешно перенесены в актуальную таблицу")
    
    # Создаем задачу внутри контекста DAG
    transfer_task = PythonOperator(
        task_id='transfer_to_actual',
        python_callable=transfer,
    )

# DAG автоматически регистрируется благодаря конструкции 'with DAG() as dag:'