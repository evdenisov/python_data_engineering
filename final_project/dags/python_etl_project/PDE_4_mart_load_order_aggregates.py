from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def load_order_aggregates():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Очищаем таблицу перед загрузкой
        cursor.execute("TRUNCATE TABLE hse_python_etl_project.order_aggregates;")
        conn.commit()
        print("🗑️ Таблица order_aggregates очищена")
        
        # Выполняем агрегацию и вставку
        cursor.execute("""
            WITH aggr_orders AS (
                SELECT 
                    t1.order_id,
                    t1.created_at::DATE AS created_at,
                    t1.user_id,
                    t1.driver_id,
                    t1.order_discount,
                    t1.delivery_cost,
                    t4.store_name,
                    t4.store_city,
                    MAX(CASE WHEN t5.status = 'delivered' THEN 1 ELSE 0 END) AS is_delivered,
                    MAX(CASE WHEN t6.status = 'canceled' THEN 1 ELSE 0 END) AS is_canceled,
                    MAX(CASE WHEN t6.status = 'canceled' THEN t6.reason ELSE NULL END) AS cancel_reason,
                    SUM(
                        (t2.item_quantity) * t3.item_price * 
                        ((100 - COALESCE(t2.item_discount, 0)) / 100::NUMERIC) * 
                        ((100 - COALESCE(t1.order_discount, 0)) / 100::NUMERIC)
                    ) AS turnover,
                    SUM(
                        (t2.item_quantity - COALESCE(t2.item_canceled_quantity, 0)) * t3.item_price * 
                        ((100 - COALESCE(t2.item_discount, 0)) / 100::NUMERIC) * 
                        ((100 - COALESCE(t1.order_discount, 0)) / 100::NUMERIC)
                    ) AS revenue,
                    MAX(t1.delivery_cost) AS cost
                FROM hse_python_etl_project.orders t1
                LEFT JOIN hse_python_etl_project.order_items t2 ON t1.order_id = t2.order_id
                LEFT JOIN hse_python_etl_project.items t3 ON t2.item_id = t3.item_id
                LEFT JOIN hse_python_etl_project.stores t4 ON t1.store_id = t4.store_id
                LEFT JOIN hse_python_etl_project.order_status_history t5 
                    ON t1.order_id = t5.order_id AND t5.status = 'delivered'
                LEFT JOIN hse_python_etl_project.order_status_history t6 
                    ON t1.order_id = t6.order_id AND t6.status = 'canceled'
                GROUP BY 
                    t1.order_id,
                    t1.created_at::DATE,
                    t1.user_id,
                    t1.driver_id,
                    t1.order_discount,
                    t1.delivery_cost,
                    t4.store_name,
                    t4.store_city
            )
            INSERT INTO hse_python_etl_project.order_aggregates (
                year, month, day,
                store_city, store_name,
                turnover, revenue, profit,
                order_qnt, delivered_order_qnt, cancelled_order_qnt,
                cancelled_after_delivery_order_qnt, cancelled_service_order_qnt,
                user_qnt, drivers_qnt,
                avg_revenue, orders_per_user, revenue_per_user
            )
            SELECT 
                EXTRACT('year' FROM created_at) AS year,
                EXTRACT('month' FROM created_at) AS month,
                EXTRACT('day' FROM created_at) AS day,
                store_city,
                store_name,
                SUM(turnover) AS turnover,
                SUM(revenue) AS revenue,
                SUM(revenue) - SUM(cost) AS profit,
                COUNT(DISTINCT order_id) AS order_qnt,
                COUNT(DISTINCT CASE WHEN is_delivered = 1 THEN order_id END) AS delivered_order_qnt,
                COUNT(DISTINCT CASE WHEN is_canceled = 1 THEN order_id END) AS cancelled_order_qnt,
                COUNT(DISTINCT CASE WHEN is_canceled = 1 AND is_delivered = 1 THEN order_id END) AS cancelled_after_delivery_order_qnt,
                COUNT(DISTINCT CASE WHEN cancel_reason IN ('Проблемы с оплатой', 'Ошибка приложения') THEN order_id END) AS cancelled_service_order_qnt,
                COUNT(DISTINCT user_id) AS user_qnt,
                COUNT(DISTINCT driver_id) AS drivers_qnt,
                AVG(revenue) AS avg_revenue,
                COUNT(DISTINCT order_id)::NUMERIC / NULLIF(COUNT(DISTINCT user_id), 0) AS orders_per_user,
                SUM(revenue) / NULLIF(COUNT(DISTINCT user_id), 0) AS revenue_per_user
            FROM aggr_orders
            GROUP BY 
                EXTRACT('year' FROM created_at),
                EXTRACT('month' FROM created_at),
                EXTRACT('day' FROM created_at),
                store_city,
                store_name
            ORDER BY year, month, day, store_city, store_name;
        """)
        
        conn.commit()
        
        # Получаем статистику
        cursor.execute("SELECT COUNT(*) FROM hse_python_etl_project.order_aggregates;")
        count = cursor.fetchone()[0]
        print(f"✅ Загружено {count} записей в order_aggregates")
        
        # Выводим пример данных
        cursor.execute("""
            SELECT year, month, day, store_city, store_name, 
                   order_qnt, revenue, profit
            FROM hse_python_etl_project.order_aggregates
            LIMIT 5;
        """)
        
        print("\n📋 Пример загруженных данных:")
        for row in cursor.fetchall():
            print(f"   Дата: {row[0]}-{row[1]}-{row[2]}, Город: {row[3]}, Магазин: {row[4]}, "
                  f"Заказов: {row[5]}, Выручка: {row[6]:.2f}, Прибыль: {row[7]:.2f}")
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

dag = DAG(
    'PDE_4_mart_load_order_aggregates',
    default_args=default_args,
    description='Загрузка агрегированных данных по заказам',
    schedule_interval='@once',
    catchup=False,
    tags=['hse', 'aggregates', 'load'],
)

load_task = PythonOperator(
    task_id='load_order_aggregates',
    python_callable=load_order_aggregates,
    dag=dag,
)