from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Функция должна быть определена ВНЕ или иметь правильное имя
def transfer():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = """
    -- Создаем таблицу если не существует
    CREATE TABLE IF NOT EXISTS temperature_extremes_hist (
        id SERIAL PRIMARY KEY,
        hottest_date DATE,
        coldest_date DATE,
        load_date TIMESTAMP,
        processed_date DATE DEFAULT CURRENT_DATE
    );
    
    -- Вставляем новые данные
    INSERT INTO temperature_extremes_hist 
        (hottest_date, coldest_date, load_date)
    SELECT hottest_date, coldest_date, load_date 
    FROM temperature_extremes;
    
    -- Очищаем исходную таблицу (опционально, раскомментируйте если нужно)
    -- TRUNCATE TABLE temperature_extremes;
    """
    
    hook.run(sql)
    print("✅ Данные успешно перенесены в историческую таблицу")

# Правильное определение DAG
with DAG(
    'temp_history',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
    },
    schedule_interval=None,  # Только по триггеру
    catchup=False,
    is_paused_upon_creation=False,
    description='Перенос данных в историческую таблицу',
    tags=['triggered'],
) as dag:
    
    # Создаем задачу внутри контекста DAG
    transfer_task = PythonOperator(
        task_id='transfer_to_history',
        python_callable=transfer,  # Исправлено: transfer вместо transfer_to_history
    )
    
    # Триггер финального DAG
    trigger_temp_actual = TriggerDagRunOperator(
        task_id='trigger_temp_actual',
        trigger_dag_id='temp_actual',
        wait_for_completion=False,
    )
    
    # Цепочка выполнения
    transfer_task >> trigger_temp_actual