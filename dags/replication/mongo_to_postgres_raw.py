from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
import logging
from bson import json_util
import json

default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
}

def load_all_data(**context):
    """Загружает все коллекции за один раз"""
    logging.info("Начало загрузки всех данных из MongoDB")
    
    # Соответствие коллекций и таблиц
    mapping = {
        'supportTickets': 'raw.support_tickets',
        'eventLogs': 'raw.event_logs',
        'userSessions': 'raw.user_sessions',
        'moderationQueue': 'raw.moderation_queue',
        'userRecommendations': 'raw.user_recommendations'
    }
    
    try:
        # MongoDB
        mongo_hook = MongoHook(conn_id='mongo_default')
        mongo_client = mongo_hook.get_conn()
        db = mongo_client['sourcedb']
        
        # PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        pg_conn = pg_hook.get_conn()
        pg_cursor = pg_conn.cursor()
        
        # Загружаем каждую коллекцию
        total = 0
        for collection_name, table_name in mapping.items():
            logging.info(f"Загрузка {collection_name}")
            
            docs = list(db[collection_name].find())
            logging.info(f"  Найдено {len(docs)} документов")
            
            for doc in docs:
                mongo_id = str(doc['_id'])
                doc_json = json.loads(json_util.dumps(doc))
                
                pg_cursor.execute(f"""
                    INSERT INTO {table_name} (data, mongo_id, loaded_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (mongo_id) DO NOTHING
                """, (json.dumps(doc_json), mongo_id))
            
            total += len(docs)
        
        pg_conn.commit()
        logging.info(f"✅ Всего загружено: {total} записей")
        
        pg_cursor.close()
        pg_conn.close()
        mongo_client.close()
        
        return total
        
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")
        raise

# DAG с одной задачей
dag = DAG(
    'mongo_to_postgres_raw',
    default_args=default_args,
    description='Загрузка из MongoDB',
    schedule_interval='*/15 * * * *',
    catchup=False,
    tags=['mongodb', 'postgresql'],
)

load_task = PythonOperator(
    task_id='load_all_collections',
    python_callable=load_all_data,
    dag=dag,
)

trigger_transformation = TriggerDagRunOperator(
    task_id='trigger_raw_to_transformed',
    trigger_dag_id='raw_to_transformed',
    wait_for_completion=False,  # Не ждать завершения
    allowed_states=['success'],
    dag=dag,  # ваш текущий DAG
)

load_task >> trigger_transformation  # после загрузки запускаем трансформацию