import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os

# Определите start_date в default_args ИЛИ непосредственно в DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  # ОБЯЗАТЕЛЬНО должен быть
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG с start_date
dag = DAG(
    'iot_temperature_processing',
    default_args=default_args,  # Передаем default_args с start_date
    description='Обработка температурных данных IOT и загрузка в PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False,
    tags=['temperature', 'iot', 'postgres'],
)

def process_and_load_to_postgres():
    """
    Обработка данных и загрузка в PostgreSQL
    """
    # В контейнере Airflow пути другие
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(dag_folder, 'data', 'IOT-temp.csv')
    
    print(f"Ищу файл по пути: {csv_path}")
    
    # Проверяем существование файла
    if not os.path.exists(csv_path):
        print(f"Файл не найден. Проверяю альтернативные пути...")
        
        # Пробуем другие возможные пути
        possible_paths = [
            '/opt/airflow/dags/data/IOT-temp.csv',
            '/opt/airflow/dags/IOT-temp.csv',
            '/opt/airflow/data/IOT-temp.csv',
            os.path.join('/opt/airflow', csv_path),
        ]
        
        for path in possible_paths:
            print(f"Проверяю: {path}")
            if os.path.exists(path):
                csv_path = path
                print(f"✅ Файл найден: {csv_path}")
                break
        else:
            raise FileNotFoundError(f"Файл IOT-temp.csv не найден ни по одному из путей")
    
    print(f"Читаю CSV файл: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Файл прочитан успешно. Строк: {len(df)}")
    
    # Продолжаем обработку
    print("Преобразую даты...")
    df['date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M').dt.date
    
    print("Фильтрую внутренние температуры...")
    df_new = df[df['out/in'] == 'In']
    print(f"После фильтрации 'In': {len(df_new)} строк")
    
    print("Рассчитываю перцентили...")
    df_new['percentile_rank'] = df_new['temp'].rank(pct=True) * 100
    
    print("Удаляю выбросы...")
    df_newest = df_new[(df_new['percentile_rank'] > 5) & (df_new['percentile_rank'] < 95)]
    print(f"После удаления выбросов: {len(df_newest)} строк")
    
    print("Агрегирую по дням...")
    df_newest_final = df_newest.groupby('date')['temp'].agg(['max', 'min']).reset_index()
    print(f"Агрегировано по {len(df_newest_final)} дням")
    
    print("Определяю самые жаркие дни...")
    df_hottest = df_newest_final.sort_values(by='max', ascending=False)['date'].head().reset_index(drop=True)
    
    print("Определяю самые холодные дни...")
    df_coldest = df_newest_final.sort_values(by='min', ascending=True)['date'].head().reset_index(drop=True)
    
    print("Создаю итоговый DataFrame...")
    df_total = pd.DataFrame({
        'hottest_date': df_hottest,
        'coldest_date': df_coldest
    })
    
    print(f"Итоговый DataFrame ({len(df_total)} строк):")
    print(df_total)
    
    # Загрузка в PostgreSQL
    if len(df_total) == 0:
        print("⚠ Внимание: DataFrame пустой, нет данных для загрузки")
        return
    
    print("Добавляю дату загрузки...")
    df_total['load_date'] = datetime.now()
    df_total['created_at'] = datetime.now()
    
    print("Подключаюсь к PostgreSQL...")
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    # Проверяем подключение
    try:
        with engine.connect() as conn:
            result = conn.execute("SELECT version()").fetchone()
            print(f"✅ Подключение к PostgreSQL успешно. Версия: {result[0][:50]}...")
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        raise
    
    print("Записываю в таблицу temperature_extremes...")
    
    # Сначала пробуем удалить таблицу если существует (для if_exists='replace')
    try:
        with engine.connect() as conn:
            conn.execute("DROP TABLE IF EXISTS temperature_extremes")
            print("Старая таблица удалена (если существовала)")
    except Exception as e:
        print(f"Ошибка при удалении таблицы: {e}")
    
    # Создаем новую таблицу
    df_total.to_sql(
        'temperature_extremes',
        engine,
        if_exists='replace',
        index=False
    )
    
    print(f"✅ Успешно создана таблица temperature_extremes с {len(df_total)} записями")
    
    # Проверяем что таблица создана
    with engine.connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM temperature_extremes").scalar()
        print(f"✅ Проверка: в таблице {count} записей")

# Задача обработки
process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_and_load_to_postgres,
    dag=dag,
)

# Триггер следующего DAG
trigger_history = TriggerDagRunOperator(
    task_id='trigger_temp_history',
    trigger_dag_id='temp_history',
    wait_for_completion=False,
    execution_date='{{ execution_date }}',
    reset_dag_run=True,
    dag=dag,
)

# Цепочка выполнения
process_task >> trigger_history