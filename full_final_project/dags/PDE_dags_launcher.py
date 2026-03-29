from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'PDE_launcher',
    default_args=default_args,
    description='Оркестратор для последовательного запуска всех DAG',
    schedule_interval='@once',
    catchup=False,
    tags=['hse', 'orchestrator', 'pipeline'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    # 1. Создание схемы и таблиц
    trigger_create = TriggerDagRunOperator(
        task_id='trigger_create_schema_and_tables',
        trigger_dag_id='PDE_0_create_schema_and_tables',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # 2. Загрузка parquet данных
    trigger_extract = TriggerDagRunOperator(
        task_id='trigger_extract_parquet_data',
        trigger_dag_id='PDE_1_extract_parquet_data',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # 3. Трансформация данных
    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform_raw_data',
        trigger_dag_id='PDE_2_transform_raw_data',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # 4. Загрузка в 3NF
    trigger_load_3nf = TriggerDagRunOperator(
        task_id='trigger_load_data_to_3nf',
        trigger_dag_id='PDE_3_load_data_to_3nf',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # 5. Загрузка агрегатов заказов
    trigger_order_aggregates = TriggerDagRunOperator(
        task_id='trigger_order_aggregates',
        trigger_dag_id='PDE_4_mart_load_order_aggregates',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    # 6. Загрузка агрегатов товаров
    trigger_item_aggregates = TriggerDagRunOperator(
        task_id='trigger_item_aggregates',
        trigger_dag_id='PDE_5_load_items_aggregates',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    end = DummyOperator(task_id='end')
    
    # Последовательность запуска
    start >> trigger_create >> trigger_extract >> trigger_transform >> trigger_load_3nf >> trigger_order_aggregates >> trigger_item_aggregates >> end