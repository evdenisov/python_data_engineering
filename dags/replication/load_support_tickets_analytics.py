from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    'load_support_tickets_analytics',
    description='Загрузка аналитики по тикетам',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['mart'],
) as dag:

    load_analytics = PostgresOperator(
        task_id='load_tickets_analytics',
        postgres_conn_id='postgres_default',
        sql="""
        -- Создаем таблицу если не существует
        CREATE TABLE IF NOT EXISTS mart.support_tickets_analytics (
            ticket_id VARCHAR(50) PRIMARY KEY,
            status VARCHAR(50),
            issue_type VARCHAR(100),
            resolve_duration INTERVAL
        );
        
        -- Очищаем таблицу
        TRUNCATE TABLE mart.support_tickets_analytics;
        
        -- Загружаем данные
        INSERT INTO mart.support_tickets_analytics (
            ticket_id, status, issue_type, resolve_duration
        )
        SELECT 
            mongo_id as ticket_id,
            status,
            issue_type,
            CASE 
                WHEN status = 'resolved' THEN updated_at - created_at
                ELSE NULL 
            END as resolve_duration
        FROM raw.transformed_support_tickets;
        
        -- Создаем индексы
        CREATE INDEX IF NOT EXISTS idx_tickets_analytics_status ON mart.support_tickets_analytics(status);
        CREATE INDEX IF NOT EXISTS idx_tickets_analytics_issue_type ON mart.support_tickets_analytics(issue_type);
        """
    )