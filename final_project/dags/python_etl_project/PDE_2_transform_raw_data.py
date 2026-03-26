from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2024, 1, 1),
}

def transform_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE hse_python_etl_project.transformed_data;")
    
    cursor.execute("""
        INSERT INTO hse_python_etl_project.transformed_data (
            order_id, user_id, store_id, driver_id,
            item_id, item_title, item_category, item_price, item_quantity,
            item_discount, item_canceled_quantity, item_replaced_id,
            delivery_cost, order_discount, payment_type,
            address_text, store_address, user_phone, driver_phone,
            created_at, paid_at, delivered_at, delivery_started_at, canceled_at,
            order_cancellation_reason
        )
        SELECT
            (row_data->>'order_id')::BIGINT,
            (row_data->>'user_id')::BIGINT,
            (row_data->>'store_id')::INTEGER,
            (row_data->>'driver_id')::INTEGER,
            (row_data->>'item_id')::BIGINT,
            row_data->>'item_title',
            row_data->>'item_category',
            (row_data->>'item_price')::NUMERIC(10,2),
            (row_data->>'item_quantity')::INTEGER,
            (row_data->>'item_discount')::NUMERIC(10,2),
            (row_data->>'item_canceled_quantity')::INTEGER,
            CASE 
                WHEN row_data->>'item_replaced_id' IS NOT NULL 
                AND row_data->>'item_replaced_id' != 'null'
                THEN (row_data->>'item_replaced_id')::NUMERIC::BIGINT
                ELSE NULL
            END,
            (row_data->>'delivery_cost')::NUMERIC(10,2),
            (row_data->>'order_discount')::NUMERIC(10,2),
            row_data->>'payment_type',
            row_data->>'address_text',
            row_data->>'store_address',
            -- Телефон пользователя: очистка и замена 7 на 8
            regexp_replace(
                regexp_replace(row_data->>'user_phone', '[^0-9]', '', 'g'),
                '^7',
                '8'
            ) as user_phone,
            -- Телефон водителя: очистка и замена 7 на 8
            regexp_replace(
                regexp_replace(row_data->>'driver_phone', '[^0-9]', '', 'g'),
                '^7',
                '8'
            ) as driver_phone,
            (row_data->>'created_at')::TIMESTAMP,
            NULLIF(row_data->>'paid_at', 'null')::TIMESTAMP,
            NULLIF(row_data->>'delivered_at', 'null')::TIMESTAMP,
            NULLIF(row_data->>'delivery_started_at', 'null')::TIMESTAMP,
            NULLIF(row_data->>'canceled_at', 'null')::TIMESTAMP,
            row_data->>'order_cancellation_reason'
        FROM hse_python_etl_project.raw_parquet_data
        WHERE row_data->>'order_id' IS NOT NULL;
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Данные трансформированы")

dag = DAG(
    'PDE_2_transform_raw_data',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)