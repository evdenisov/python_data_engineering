from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

def create_objects():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Создаем схему
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS hse_python_etl_project;
        """)
        
        # Удаляем существующие таблицы (в правильном порядке - сначала дочерние)
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.item_replacements CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.order_status_history CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.order_items CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.orders CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.users CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.drivers CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.stores CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.items CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.order_aggregates CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.items_aggregates CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.transformed_data CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS hse_python_etl_project.raw_parquet_data CASCADE;")
        conn.commit()
        print("🗑️ Старые таблицы удалены")
        
        # Таблица для сырых данных
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.raw_parquet_data (
                id SERIAL PRIMARY KEY,
                file_name VARCHAR(255),
                row_data JSONB NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Таблица для трансформированных данных
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.transformed_data (
                id SERIAL PRIMARY KEY,
                order_id BIGINT,
                user_id BIGINT,
                store_id INTEGER,
                driver_id INTEGER,
                item_id BIGINT,
                item_title VARCHAR(255),
                item_category VARCHAR(100),
                item_price NUMERIC(10, 2),
                item_quantity INTEGER,
                item_discount NUMERIC(10, 2),
                item_canceled_quantity INTEGER,
                item_replaced_id BIGINT,
                delivery_cost NUMERIC(10, 2),
                order_discount NUMERIC(10, 2),
                payment_type VARCHAR(100),
                address_text TEXT,
                store_address TEXT,
                user_phone VARCHAR(50),
                driver_phone VARCHAR(50),
                created_at TIMESTAMP,
                paid_at TIMESTAMP,
                delivered_at TIMESTAMP,
                delivery_started_at TIMESTAMP,
                canceled_at TIMESTAMP,
                order_cancellation_reason TEXT,
                transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # 1. Таблица: пользователи
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.users (
                user_id BIGINT PRIMARY KEY,
                user_phone VARCHAR(50)
            );
        """)
        
        # 2. Таблица: водители
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.drivers (
                driver_id BIGINT PRIMARY KEY,
                driver_phone VARCHAR(50)
            );
        """)
        
        # 3. Таблица: магазины (обновленная структура)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.stores (
                store_id INTEGER PRIMARY KEY,
                store_name VARCHAR(255),
                store_city VARCHAR(100),
                store_full_address TEXT
            );
        """)
        
        # 4. Таблица: товары
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.items (
                item_id BIGINT PRIMARY KEY,
                item_title VARCHAR(255),
                item_category VARCHAR(100),
                item_price NUMERIC(10, 2)
            );
        """)
        
        # 5. Таблица: заказы
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.orders (
                order_id BIGINT PRIMARY KEY,
                user_id BIGINT REFERENCES hse_python_etl_project.users(user_id),
                store_id INTEGER REFERENCES hse_python_etl_project.stores(store_id),
                driver_id BIGINT REFERENCES hse_python_etl_project.drivers(driver_id),
                payment_type VARCHAR(100),
                address_text TEXT,
                order_discount NUMERIC(10, 2),
                delivery_cost NUMERIC(10, 2),
                created_at TIMESTAMP
            );
        """)
        
        # 6. Таблица: детали заказа
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.order_items (
                id SERIAL PRIMARY KEY,
                order_id BIGINT REFERENCES hse_python_etl_project.orders(order_id),
                item_id BIGINT REFERENCES hse_python_etl_project.items(item_id),
                item_quantity INTEGER,
                item_discount NUMERIC(10, 2),
                item_canceled_quantity INTEGER,
                item_replaced_id BIGINT
            );
        """)
        
        # 7. Таблица: статусы заказа
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.order_status_history (
                id SERIAL PRIMARY KEY,
                order_id BIGINT REFERENCES hse_python_etl_project.orders(order_id),
                status VARCHAR(50),
                status_time TIMESTAMP,
                reason TEXT
            );
        """)
        
        # 8. Таблица: замены товаров
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.item_replacements (
                id SERIAL PRIMARY KEY,
                order_item_id INTEGER REFERENCES hse_python_etl_project.order_items(id),
                original_item_id BIGINT,
                replaced_item_id BIGINT
            );
        """)
        
        # 9. Таблица: агрегированные данные по заказам
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.order_aggregates (
                id SERIAL PRIMARY KEY,
                
                -- Временные измерения
                year INTEGER,
                month INTEGER,
                day INTEGER,
                
                -- Измерения магазина
                store_city VARCHAR(100),
                store_name VARCHAR(255),
                
                -- Метрики
                turnover NUMERIC(20, 2),
                revenue NUMERIC(20, 2),
                profit NUMERIC(20, 2),
                
                -- Количественные метрики
                order_qnt INTEGER,
                delivered_order_qnt INTEGER,
                cancelled_order_qnt INTEGER,
                cancelled_after_delivery_order_qnt INTEGER,
                cancelled_service_order_qnt INTEGER,
                
                user_qnt INTEGER,
                drivers_qnt INTEGER,
                
                -- Средние метрики
                avg_revenue NUMERIC(20, 2),
                orders_per_user NUMERIC(10, 2),
                revenue_per_user NUMERIC(20, 2),
                
                -- Метаданные
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # 10. Таблица: витрина товаров
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hse_python_etl_project.items_aggregates (
            id SERIAL PRIMARY KEY,
    
            -- Временные разрезы
            year INTEGER,
            month INTEGER,
            week INTEGER,           -- номер недели
            day INTEGER,
            
            -- Географические разрезы
            store_city VARCHAR(100),
            store_name VARCHAR(255),
            
            -- Товарные разрезы
            item_category VARCHAR(100),
            item_id BIGINT,
            item_title VARCHAR(255),
            
            -- Метрики
            product_turnover NUMERIC(20, 2),           -- Оборот товара
            ordered_units INTEGER,                      -- Количество заказанных единиц товара
            canceled_units INTEGER,                     -- Количество отмененных единиц товара
            orders_with_product INTEGER,                -- Количество заказов с товаром
            orders_with_canceled_product INTEGER,       -- Количество заказов с отменой товара
            
            -- Рейтинги популярности
            is_top_product_day BOOLEAN DEFAULT FALSE,   -- Самый популярный товар за день
            is_top_product_week BOOLEAN DEFAULT FALSE,  -- Самый популярный товар за неделю
            is_top_product_month BOOLEAN DEFAULT FALSE, -- Самый популярный товар за месяц
            
            -- Метаданные
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        conn.commit()
        print("✅ Все таблицы созданы успешно")
        
        # Выводим список созданных таблиц
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'hse_python_etl_project'
            ORDER BY table_name;
        """)
        
        print("\n📋 Созданные таблицы:")
        tables = cursor.fetchall()
        for row in tables:
            print(f"   - {row[0]}")
        print(f"\n📊 Всего создано таблиц: {len(tables)}")
        
    except Exception as e:
        print(f"❌ Ошибка при создании объектов: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

dag = DAG(
    'PDE_0_create_schema_and_tables',
    default_args=default_args,
    description='Создание схемы и таблиц для HSE ETL проекта',
    schedule_interval='@once',
    catchup=False,
    tags=['hse', 'postgres', 'ddl'],
)

create_task = PythonOperator(
    task_id='create_objects',
    python_callable=create_objects,
    dag=dag,
)