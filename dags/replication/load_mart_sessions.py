from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    'load_mart_sessions',
    description='Загрузка данных в mart таблицы',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['mart'],
) as dag:

    # Загрузка основной таблицы (без actions_count и pages_visited_count)
    load_main = PostgresOperator(
        task_id='load_sessions_main',
        postgres_conn_id='postgres_default',
        sql="""
        TRUNCATE TABLE mart.user_sessions_main CASCADE;
        
        INSERT INTO mart.user_sessions_main (
            session_uuid, user_id, device, 
            start_time, end_time, duration_seconds
        )
        SELECT 
            session_uuid,
            user_id,
            device,
            start_time,
            end_time,
            duration_seconds
        FROM raw.transformed_user_sessions
        WHERE session_uuid IS NOT NULL;
        """
    )

    # Загрузка действий
    load_actions = PostgresOperator(
        task_id='load_actions',
        postgres_conn_id='postgres_default',
        sql="""
        DELETE FROM mart.user_session_details WHERE detail_type = 'action';
        
        INSERT INTO mart.user_session_details (session_uuid, detail_type, detail_value)
        SELECT 
            session_uuid,
            'action',
            unnest(string_to_array(actions, ','))
        FROM raw.transformed_user_sessions
        WHERE actions IS NOT NULL AND actions != '';
        """
    )

    # Загрузка страниц
    load_pages = PostgresOperator(
        task_id='load_pages',
        postgres_conn_id='postgres_default',
        sql="""
        DELETE FROM mart.user_session_details WHERE detail_type = 'page';
        
        INSERT INTO mart.user_session_details (session_uuid, detail_type, detail_value)
        SELECT 
            session_uuid,
            'page',
            unnest(string_to_array(pages_visited, ','))
        FROM raw.transformed_user_sessions
        WHERE pages_visited IS NOT NULL AND pages_visited != '';
        """
    )

    load_main >> [load_actions, load_pages]