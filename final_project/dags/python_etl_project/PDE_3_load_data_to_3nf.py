from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def load_to_3nf():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Очищаем таблицы в правильном порядке (сначала дочерние, потом родительские)
        cursor.execute("TRUNCATE TABLE hse_python_etl_project.item_replacements CASCADE;")
        cursor.execute("TRUNCATE TABLE hse_python_etl_project.order_status_history CASCADE;")
        cursor.execute("TRUNCATE TABLE hse_python_etl_project.order_items CASCADE;")
        cursor.execute("TRUNCATE TABLE hse_python_etl_project.orders CASCADE;")
        cursor.execute("TRUNCATE TABLE hse_python_etl_project.users CASCADE;")
        cursor.execute("TRUNCATE TABLE hse_python_etl_project.drivers CASCADE;")
        cursor.execute("TRUNCATE TABLE hse_python_etl_project.stores CASCADE;")
        cursor.execute("TRUNCATE TABLE hse_python_etl_project.items CASCADE;")
        conn.commit()
        print("🗑️ Таблицы очищены (с каскадным удалением)")
        
        # 1. Загружаем пользователей
        cursor.execute("""
            INSERT INTO hse_python_etl_project.users (user_id, user_phone)
            SELECT DISTINCT user_id, user_phone
            FROM hse_python_etl_project.transformed_data
            WHERE user_id IS NOT NULL
            ON CONFLICT (user_id) DO NOTHING;
        """)
        print(f"✅ Загружены users: {cursor.rowcount} записей")
        
        # 2. Загружаем водителей
        cursor.execute("""
            INSERT INTO hse_python_etl_project.drivers (driver_id, driver_phone)
            SELECT DISTINCT driver_id, driver_phone
            FROM hse_python_etl_project.transformed_data
            WHERE driver_id IS NOT NULL
            ON CONFLICT (driver_id) DO NOTHING;
        """)
        print(f"✅ Загружены drivers: {cursor.rowcount} записей")
        
          # 3. Загружаем магазины (с парсингом адреса)
        cursor.execute("""
            INSERT INTO hse_python_etl_project.stores (store_id, store_name, store_city, store_full_address)
            SELECT 
                store_id,
                -- Извлекаем название магазина (часть до запятой)
                SPLIT_PART(store_address, ',', 1) as store_name,
                -- Извлекаем город (часть после первой запятой, до второй)
                TRIM(SPLIT_PART(store_address, ',', 2)) as store_city,
                -- Полный адрес
                store_address as store_full_address
            FROM (
                SELECT DISTINCT store_id, store_address
                FROM hse_python_etl_project.transformed_data
                WHERE store_id IS NOT NULL
            ) as distinct_stores
            ON CONFLICT (store_id) DO NOTHING;
        """)
        print(f"✅ Загружены stores: {cursor.rowcount} записей")
        
        # 4. Загружаем товары
        cursor.execute("""
            INSERT INTO hse_python_etl_project.items (item_id, item_title, item_category, item_price)
            SELECT DISTINCT item_id, item_title, item_category, item_price
            FROM hse_python_etl_project.transformed_data
            WHERE item_id IS NOT NULL
            ON CONFLICT (item_id) DO NOTHING;
        """)
        print(f"✅ Загружены items: {cursor.rowcount} записей")
        
        conn.commit()
        
        # 5. Загружаем заказы
        cursor.execute("""
            INSERT INTO hse_python_etl_project.orders (
                order_id, user_id, store_id, driver_id,
                payment_type, address_text, order_discount, delivery_cost, created_at
            )
            SELECT DISTINCT 
                order_id, user_id, store_id, driver_id,
                payment_type, address_text, order_discount, delivery_cost, created_at
            FROM hse_python_etl_project.transformed_data
            WHERE order_id IS NOT NULL
            ON CONFLICT (order_id) DO NOTHING;
        """)
        print(f"✅ Загружены orders: {cursor.rowcount} записей")
        
        # 6. Загружаем детали заказов
        cursor.execute("""
            INSERT INTO hse_python_etl_project.order_items (
                order_id, item_id, item_quantity, item_discount,
                item_canceled_quantity, item_replaced_id
            )
            SELECT 
                order_id, 
                item_id, 
                SUM(item_quantity) as item_quantity,
                item_discount,
                SUM(item_canceled_quantity) as item_canceled_quantity,
                item_replaced_id
            FROM hse_python_etl_project.transformed_data
            WHERE order_id IS NOT NULL AND item_id IS NOT NULL
            GROUP BY order_id, item_id, item_discount, item_replaced_id;
        """)
        print(f"✅ Загружены order_items: {cursor.rowcount} записей")
        
        # 7. Загружаем историю статусов
        cursor.execute("""
            INSERT INTO hse_python_etl_project.order_status_history (order_id, status, status_time, reason)
            SELECT DISTINCT order_id, 'created', created_at, NULL 
            FROM hse_python_etl_project.transformed_data 
            WHERE created_at IS NOT NULL
            UNION ALL
            SELECT DISTINCT order_id, 'paid', paid_at, NULL 
            FROM hse_python_etl_project.transformed_data 
            WHERE paid_at IS NOT NULL
            UNION ALL
            SELECT DISTINCT order_id, 'delivery_started', delivery_started_at, NULL 
            FROM hse_python_etl_project.transformed_data 
            WHERE delivery_started_at IS NOT NULL
            UNION ALL
            SELECT DISTINCT order_id, 'delivered', delivered_at, NULL 
            FROM hse_python_etl_project.transformed_data 
            WHERE delivered_at IS NOT NULL
            UNION ALL
            SELECT DISTINCT order_id, 'canceled', canceled_at, order_cancellation_reason 
            FROM hse_python_etl_project.transformed_data 
            WHERE canceled_at IS NOT NULL;
        """)
        print(f"✅ Загружена order_status_history: {cursor.rowcount} записей")
        
        # 8. Загружаем замены товаров
        cursor.execute("""
            INSERT INTO hse_python_etl_project.item_replacements (order_item_id, original_item_id, replaced_item_id)
            SELECT oi.id, oi.item_replaced_id, oi.item_id
            FROM hse_python_etl_project.order_items oi
            WHERE oi.item_replaced_id IS NOT NULL;
        """)
        print(f"✅ Загружены item_replacements: {cursor.rowcount} записей")
        
        conn.commit()
        
        # Выводим статистику
        cursor.execute("""
            SELECT 'users' as table_name, COUNT(*) FROM hse_python_etl_project.users
            UNION ALL
            SELECT 'drivers', COUNT(*) FROM hse_python_etl_project.drivers
            UNION ALL
            SELECT 'stores', COUNT(*) FROM hse_python_etl_project.stores
            UNION ALL
            SELECT 'items', COUNT(*) FROM hse_python_etl_project.items
            UNION ALL
            SELECT 'orders', COUNT(*) FROM hse_python_etl_project.orders
            UNION ALL
            SELECT 'order_items', COUNT(*) FROM hse_python_etl_project.order_items
            UNION ALL
            SELECT 'order_status_history', COUNT(*) FROM hse_python_etl_project.order_status_history
            UNION ALL
            SELECT 'item_replacements', COUNT(*) FROM hse_python_etl_project.item_replacements;
        """)
        
        print("\n📊 СТАТИСТИКА ЗАГРУЗКИ:")
        for row in cursor.fetchall():
            print(f"   {row[0]}: {row[1]} записей")
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

dag = DAG(
    'PDE_3_load_data_to_3nf',
    default_args=default_args,
    description='Загрузка данных в нормализованные таблицы 3NF',
    schedule_interval='@once',
    catchup=False,
    tags=['hse', '3nf', 'load'],
)

load_task = PythonOperator(
    task_id='load_to_3nf',
    python_callable=load_to_3nf,
    dag=dag,
)