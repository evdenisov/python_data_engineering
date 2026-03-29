from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

def load_items_aggregates():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Очищаем таблицу перед загрузкой
        cursor.execute("TRUNCATE TABLE hse_python_etl_project.items_aggregates;")
        conn.commit()
        print("🗑️ Таблица items_aggregates очищена")
        
        # 1. Загружаем базовые метрики
        cursor.execute("""
            INSERT INTO hse_python_etl_project.items_aggregates (
                year, month, week, day,
                store_city, store_name,
                item_category, item_id, item_title,
                product_turnover, ordered_units, canceled_units,
                orders_with_product, orders_with_canceled_product
            )
            SELECT 
                EXTRACT('year' FROM t1.created_at) AS year,
                EXTRACT('month' FROM t1.created_at) AS month,
                EXTRACT('week' FROM t1.created_at) AS week,
                EXTRACT('day' FROM t1.created_at) AS day,
                t4.store_city,
                t4.store_name,
                t3.item_category,
                t2.item_id,
                t3.item_title,
                SUM((t2.item_quantity - COALESCE(t2.item_canceled_quantity, 0)) * t3.item_price) AS product_turnover,
                SUM(t2.item_quantity) AS ordered_units,
                SUM(COALESCE(t2.item_canceled_quantity, 0)) AS canceled_units,
                COUNT(DISTINCT t1.order_id) AS orders_with_product,
                COUNT(DISTINCT CASE WHEN t2.item_canceled_quantity > 0 THEN t1.order_id END) AS orders_with_canceled_product
            FROM hse_python_etl_project.orders t1
            LEFT JOIN hse_python_etl_project.order_items t2 ON t1.order_id = t2.order_id
            LEFT JOIN hse_python_etl_project.items t3 ON t2.item_id = t3.item_id
            LEFT JOIN hse_python_etl_project.stores t4 ON t1.store_id = t4.store_id
            WHERE t2.item_id IS NOT NULL
            GROUP BY 
                EXTRACT('year' FROM t1.created_at),
                EXTRACT('month' FROM t1.created_at),
                EXTRACT('week' FROM t1.created_at),
                EXTRACT('day' FROM t1.created_at),
                t4.store_city,
                t4.store_name,
                t3.item_category,
                t2.item_id,
                t3.item_title;
        """)
        conn.commit()
        print("✅ Загружены базовые метрики")
        
        # 2. Обновляем флаги самых популярных товаров за день
        cursor.execute("""
            UPDATE hse_python_etl_project.items_aggregates
            SET is_top_product_day = TRUE
            WHERE id IN (
                SELECT id FROM (
                    SELECT 
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY year, month, day, store_city, store_name
                            ORDER BY ordered_units DESC
                        ) AS rank_desc
                    FROM hse_python_etl_project.items_aggregates
                    WHERE ordered_units > 0
                ) ranked
                WHERE rank_desc = 1
            );
        """)
        conn.commit()
        print("✅ Обновлены флаги самых популярных товаров за день")
        
        # 3. Обновляем флаги самых популярных товаров за неделю
        cursor.execute("""
            UPDATE hse_python_etl_project.items_aggregates
            SET is_top_product_week = TRUE
            WHERE id IN (
                SELECT id FROM (
                    SELECT 
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY year, week, store_city, store_name
                            ORDER BY ordered_units DESC
                        ) AS rank_desc
                    FROM hse_python_etl_project.items_aggregates
                    WHERE ordered_units > 0
                ) ranked
                WHERE rank_desc = 1
            );
        """)
        conn.commit()
        print("✅ Обновлены флаги самых популярных товаров за неделю")
        
        # 4. Обновляем флаги самых популярных товаров за месяц
        cursor.execute("""
            UPDATE hse_python_etl_project.items_aggregates
            SET is_top_product_month = TRUE
            WHERE id IN (
                SELECT id FROM (
                    SELECT 
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY year, month, store_city, store_name
                            ORDER BY ordered_units DESC
                        ) AS rank_desc
                    FROM hse_python_etl_project.items_aggregates
                    WHERE ordered_units > 0
                ) ranked
                WHERE rank_desc = 1
            );
        """)
        conn.commit()
        print("✅ Обновлены флаги самых популярных товаров за месяц")
        
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

dag = DAG(
    'PDE_5_load_items_aggregates',
    default_args=default_args,
    description='Загрузка агрегатов товаров с расчетом популярности',
    schedule_interval='@once',
    catchup=False,
    tags=['hse', 'items', 'aggregates'],
)

load_task = PythonOperator(
    task_id='load_items_aggregates',
    python_callable=load_items_aggregates,
    dag=dag,
)