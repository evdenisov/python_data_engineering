"""
DAG для трансформации данных из raw таблиц в transformed таблицы с помощью SQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # ← ДОБАВИТЬ
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
}

def execute_sql_transformation(sql_query, source_table, target_table, **context):
    """
    Выполняет SQL трансформацию и логирует результат
    """
    logger.info("=" * 60)
    logger.info(f"ТРАНСФОРМАЦИЯ: {source_table} -> {target_table}")
    logger.info("=" * 60)
    
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Начинаем транзакцию
        cursor.execute("BEGIN;")
        
        # Очищаем целевую таблицу перед вставкой (или можно использовать TRUNCATE)
        cursor.execute(f"TRUNCATE TABLE {target_table};")
        
        # Выполняем трансформацию
        logger.info("Выполнение SQL трансформации...")
        cursor.execute(sql_query)
        
        # Получаем количество вставленных записей
        cursor.execute(f"SELECT COUNT(*) FROM {target_table};")
        inserted_count = cursor.fetchone()[0]
        
        # Фиксируем транзакцию
        cursor.execute("COMMIT;")
        
        # Логируем результат
        cursor.execute("""
            INSERT INTO raw.transformation_log (table_name, records_processed, status, created_at)
            VALUES (%s, %s, %s, NOW());
        """, (target_table, inserted_count, 'success'))
        
        conn.commit()
        
        logger.info(f"✅ Успешно трансформировано {inserted_count} записей в {target_table}")
        
        cursor.close()
        conn.close()
        
        return inserted_count
        
    except Exception as e:
        logger.error(f"❌ Ошибка при трансформации: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

# =====================================================
# SQL ЗАПРОСЫ ДЛЯ ТРАНСФОРМАЦИИ
# =====================================================

# Трансформация support_tickets
SQL_TRANSFORM_TICKETS = """
-- Очищаем таблицу
TRUNCATE TABLE raw.transformed_support_tickets;

-- Вставляем данные с MongoDB ID как ключ
INSERT INTO raw.transformed_support_tickets (
    mongo_id, ticket_id, user_id, status, issue_type, 
    created_at, updated_at, total_messages, extracted_at
)
SELECT 
    -- MongoDB ID как ключ
    data->'_id'->>'$oid' as mongo_id,
    
    -- Основные поля
    data->>'ticket_id' as ticket_id,
    data->>'user_id' as user_id,
    data->>'status' as status,
    data->>'issue_type' as issue_type,
    
    -- Правильное извлечение timestamp
    to_timestamp((data->'created_at'->>'$date')::bigint / 1000) as created_at,
    to_timestamp((data->'updated_at'->>'$date')::bigint / 1000) as updated_at,
    
    -- Количество сообщений
    jsonb_array_length(data->'messages') as total_messages,
    
    -- Время загрузки
    NOW() as extracted_at
    
FROM raw.support_tickets
WHERE data->>'ticket_id' IS NOT NULL
  AND data->'_id'->>'$oid' IS NOT NULL
-- Обновляем при конфликте (если запись уже существует)
ON CONFLICT (mongo_id) DO UPDATE SET
    ticket_id = EXCLUDED.ticket_id,
    user_id = EXCLUDED.user_id,
    status = EXCLUDED.status,
    issue_type = EXCLUDED.issue_type,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    total_messages = EXCLUDED.total_messages,
    extracted_at = EXCLUDED.extracted_at;
"""

# Трансформация ticket_messages (из массива messages)
SQL_TRANSFORM_TICKET_MESSAGES = """
-- Очищаем таблицу
TRUNCATE TABLE raw.transformed_ticket_messages;

-- Вставляем сообщения
INSERT INTO raw.transformed_ticket_messages (
    raw_id, ticket_id, sender, message, message_time, extracted_at
)
SELECT 
    r.id as raw_id,
    r.data->>'ticket_id' as ticket_id,
    msg->>'sender' as sender,
    msg->>'message' as message,
    
    -- Правильное извлечение timestamp из сообщения
    to_timestamp((msg->'timestamp'->>'$date')::bigint / 1000) as message_time,
    
    NOW() as extracted_at
FROM raw.support_tickets r,
     jsonb_array_elements(r.data->'messages') msg
WHERE r.data->>'ticket_id' IS NOT NULL;
"""

# Трансформация event_logs
SQL_TRANSFORM_EVENT_LOGS = """
-- Очищаем таблицу
TRUNCATE TABLE raw.transformed_event_logs;

-- Вставляем события с максимальным раскрытием JSON
INSERT INTO raw.transformed_event_logs (
    -- Основные поля
    raw_id, event_id, user_id, event_type, event_time, 
    ip_address, page_url, session_id, extracted_at,
    
    -- Раскрытые поля из details
    page_path, element_name, additional_info,
    
    -- MongoDB специфичные поля
    event_uuid,
    
    -- Для отладки сохраняем оригинальные JSON
    original__id, original_details, original_timestamp
)
SELECT 
    -- Основные поля
    id as raw_id,
    data->>'event_id' as event_id,
    data->>'user_id' as user_id,
    data->>'event_type' as event_type,
    
    -- Правильное извлечение timestamp
    to_timestamp((data->'timestamp'->>'$date')::bigint / 1000) as event_time,
    
    data->>'ip_address' as ip_address,
    data->>'page_url' as page_url,
    data->>'session_id' as session_id,
    NOW() as extracted_at,
    
    -- Раскрываем поля из details (если они существуют)
    data->'details'->>'page' as page_path,
    data->'details'->>'element' as element_name,
    data->'details'->>'additional_info' as additional_info,
    
    -- Извлекаем _id из MongoDB формата
    data->'_id'->>'$oid' as event_uuid,
    
    -- Сохраняем оригинальные JSON для отладки
    data->'_id' as original__id,
    data->'details' as original_details,
    data->'timestamp' as original_timestamp
    
FROM raw.event_logs
WHERE data->>'event_id' IS NOT NULL;
"""

# Трансформация user_sessions
SQL_TRANSFORM_USER_SESSIONS = """
-- Очищаем таблицу
TRUNCATE TABLE raw.transformed_user_sessions;

-- Вставляем сессии с массивами в виде строк
INSERT INTO raw.transformed_user_sessions (
    raw_id, session_id, user_id, device, 
    start_time, end_time, duration_seconds,
    extracted_at, actions, pages_visited,
    session_uuid, actions_count, pages_visited_count
)
SELECT 
    id as raw_id,
    data->>'session_id' as session_id,
    data->>'user_id' as user_id,
    data->>'device' as device,
    
    to_timestamp((data->'start_time'->>'$date')::bigint / 1000) as start_time,
    to_timestamp((data->'end_time'->>'$date')::bigint / 1000) as end_time,
    
    ((data->'end_time'->>'$date')::bigint - (data->'start_time'->>'$date')::bigint) / 1000 as duration_seconds,
    
    NOW() as extracted_at,
    
    -- Преобразуем JSON массивы в строки
    array_to_string(
        ARRAY(SELECT jsonb_array_elements_text(data->'actions')), 
        ','
    ) as actions,
    
    array_to_string(
        ARRAY(SELECT jsonb_array_elements_text(data->'pages_visited')), 
        ','
    ) as pages_visited,
    
    data->'_id'->>'$oid' as session_uuid,
    
    jsonb_array_length(data->'actions') as actions_count,
    jsonb_array_length(data->'pages_visited') as pages_visited_count
    
FROM raw.user_sessions
WHERE data->>'session_id' IS NOT NULL;
"""

# Трансформация moderation_queue
SQL_TRANSFORM_MODERATION = """
-- Очищаем таблицу
TRUNCATE TABLE raw.transformed_moderation_queue;

-- Вставляем данные модерации
INSERT INTO raw.transformed_moderation_queue (
    raw_id, item_id, user_id, content_type, status,
    submitted_at, reviewed_at, reason, action_taken, extracted_at
)
SELECT 
    id as raw_id,
    data->>'review_id' as item_id,
    data->>'user_id' as user_id,
    data->>'content_type' as content_type,
    data->>'moderation_status' as status,
    
    -- Правильное извлечение timestamp
    to_timestamp((data->'submitted_at'->>'$date')::bigint / 1000) as submitted_at,
    to_timestamp((data->'updated_at'->>'$date')::bigint / 1000) as reviewed_at,
    
    data->>'review_text' as reason,
    data->>'action' as action_taken,
    NOW() as extracted_at
FROM raw.moderation_queue
WHERE data->>'review_id' IS NOT NULL;
"""

# Трансформация user_recommendations
SQL_TRANSFORM_RECOMMENDATIONS = """
-- Очищаем таблицу
TRUNCATE TABLE raw.transformed_user_recommendations;

-- Вставляем данные в правильном формате
INSERT INTO raw.transformed_user_recommendations (
    raw_id, user_id, last_updated, recommended_products, is_active, extracted_at
)
SELECT 
    id as raw_id,
    data->>'user_id' as user_id,
    
    -- Извлекаем last_updated из формата {"$date": число}
    to_timestamp((data->'last_updated'->>'$date')::bigint / 1000) as last_updated,
    
    -- Преобразуем массив JSON в массив PostgreSQL
    ARRAY(
        SELECT jsonb_array_elements_text(data->'recommended_products')
    ) as recommended_products,
    
    -- Все записи активны по умолчанию
    true as is_active,
    
    NOW() as extracted_at
FROM raw.user_recommendations
WHERE data IS NOT NULL;
"""

# =====================================================
# СОЗДАНИЕ DAG
# =====================================================

dag = DAG(
    'raw_to_transformed',
    default_args=default_args,
    description='Трансформация данных из raw таблиц в transformed с помощью SQL',
    schedule_interval='*/30 * * * *',  # Каждые 30 минут
    catchup=False,
    tags=['postgresql', 'transform', 'sql'],
)

# Старт и финиш
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Задачи трансформации
transform_tickets = PythonOperator(
    task_id='transform_support_tickets',
    python_callable=execute_sql_transformation,
    op_kwargs={
        'sql_query': SQL_TRANSFORM_TICKETS,
        'source_table': 'raw.support_tickets',
        'target_table': 'raw.transformed_support_tickets'
    },
    dag=dag,
)

transform_messages = PythonOperator(
    task_id='transform_ticket_messages',
    python_callable=execute_sql_transformation,
    op_kwargs={
        'sql_query': SQL_TRANSFORM_TICKET_MESSAGES,
        'source_table': 'raw.support_tickets',
        'target_table': 'raw.transformed_ticket_messages'
    },
    dag=dag,
)

transform_events = PythonOperator(
    task_id='transform_event_logs',
    python_callable=execute_sql_transformation,
    op_kwargs={
        'sql_query': SQL_TRANSFORM_EVENT_LOGS,
        'source_table': 'raw.event_logs',
        'target_table': 'raw.transformed_event_logs'
    },
    dag=dag,
)

transform_sessions = PythonOperator(
    task_id='transform_user_sessions',
    python_callable=execute_sql_transformation,
    op_kwargs={
        'sql_query': SQL_TRANSFORM_USER_SESSIONS,
        'source_table': 'raw.user_sessions',
        'target_table': 'raw.transformed_user_sessions'
    },
    dag=dag,
)

transform_moderation = PythonOperator(
    task_id='transform_moderation_queue',
    python_callable=execute_sql_transformation,
    op_kwargs={
        'sql_query': SQL_TRANSFORM_MODERATION,
        'source_table': 'raw.moderation_queue',
        'target_table': 'raw.transformed_moderation_queue'
    },
    dag=dag,
)

transform_recommendations = PythonOperator(
    task_id='transform_user_recommendations',
    python_callable=execute_sql_transformation,
    op_kwargs={
        'sql_query': SQL_TRANSFORM_RECOMMENDATIONS,
        'source_table': 'raw.user_recommendations',
        'target_table': 'raw.transformed_user_recommendations'
    },
    dag=dag,
)

# Триггеры для запуска следующих DAG-ов
trigger_load_mart_sessions = TriggerDagRunOperator(
    task_id='trigger_load_mart_sessions',
    trigger_dag_id='load_mart_sessions',
    wait_for_completion=False,
    allowed_states=['success'],
    trigger_rule='all_success',
    dag=dag,
)

trigger_load_tickets_analytics = TriggerDagRunOperator(
    task_id='trigger_load_support_tickets_analytics',
    trigger_dag_id='load_support_tickets_analytics',
    wait_for_completion=False,
    allowed_states=['success'],
    trigger_rule='all_success',
    dag=dag,
)

# Устанавливаем зависимости (все задачи выполняются параллельно)
# Все задачи трансформации
transform_tasks = [
    transform_tickets,
    transform_messages,
    transform_events,
    transform_sessions,
    transform_moderation,
    transform_recommendations
]

# Триггеры
trigger_tasks = [trigger_load_mart_sessions, trigger_load_tickets_analytics]

# Зависимости
start >> transform_tasks

for task in transform_tasks:
    task >> trigger_tasks

for trigger in trigger_tasks:
    trigger >> end