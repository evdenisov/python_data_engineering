-- Подключаемся к существующей базе данных airflow
\c airflow;


----ПРОЕКТ ПО PYTHON DATA ENGINEERING----------
-----------------------------------------------

-- Создаем схему для проекта
CREATE SCHEMA IF NOT EXISTS hse_python_etl_project;

-- Таблица для сырых данных из parquet файлов
CREATE TABLE IF NOT EXISTS hse_python_etl_project.raw_parquet_data (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255),
    row_data JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


----ПРОЕКТ ПО ETL---------------------------------
---------------------------------------------------

-- Создаем схему для сырых данных (если не существует)
CREATE SCHEMA IF NOT EXISTS raw;

-- Таблица для сырых данных из коллекции supportTickets
CREATE TABLE IF NOT EXISTS raw.support_tickets (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    mongo_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT support_tickets_mongo_id_unique UNIQUE (mongo_id)
);

-- Таблица для сырых данных из коллекции eventLogs
CREATE TABLE IF NOT EXISTS raw.event_logs (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    mongo_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT event_logs_mongo_id_unique UNIQUE (mongo_id)
);

-- Таблица для сырых данных из коллекции userSessions
CREATE TABLE IF NOT EXISTS raw.user_sessions (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    mongo_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT user_sessions_mongo_id_unique UNIQUE (mongo_id)
);

-- Таблица для сырых данных из коллекции moderationQueue
CREATE TABLE IF NOT EXISTS raw.moderation_queue (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    mongo_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT moderation_queue_mongo_id_unique UNIQUE (mongo_id)
);

-- Таблица для сырых данных из коллекции userRecommendations
CREATE TABLE IF NOT EXISTS raw.user_recommendations (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    mongo_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT user_recommendations_mongo_id_unique UNIQUE (mongo_id)
);

-- Таблица для логирования загрузок
CREATE TABLE IF NOT EXISTS raw.load_log (
    id SERIAL PRIMARY KEY,
    collection_name VARCHAR(100),
    records_loaded INTEGER,
    status VARCHAR(50) DEFAULT 'success',
    error_message TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_support_tickets_loaded_at ON raw.support_tickets(loaded_at);
CREATE INDEX IF NOT EXISTS idx_event_logs_loaded_at ON raw.event_logs(loaded_at);
CREATE INDEX IF NOT EXISTS idx_user_sessions_loaded_at ON raw.user_sessions(loaded_at);
CREATE INDEX IF NOT EXISTS idx_moderation_queue_loaded_at ON raw.moderation_queue(loaded_at);
CREATE INDEX IF NOT EXISTS idx_user_recommendations_loaded_at ON raw.user_recommendations(loaded_at);

-- Создаем индексы на mongo_id для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_support_tickets_mongo_id ON raw.support_tickets(mongo_id);
CREATE INDEX IF NOT EXISTS idx_event_logs_mongo_id ON raw.event_logs(mongo_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_mongo_id ON raw.user_sessions(mongo_id);
CREATE INDEX IF NOT EXISTS idx_moderation_queue_mongo_id ON raw.moderation_queue(mongo_id);
CREATE INDEX IF NOT EXISTS idx_user_recommendations_mongo_id ON raw.user_recommendations(mongo_id);

-- Создаем представление для мониторинга
CREATE OR REPLACE VIEW raw.monitoring_stats AS
SELECT 
    'support_tickets' as table_name,
    COUNT(*) as record_count,
    MAX(loaded_at) as last_load
FROM raw.support_tickets
UNION ALL
SELECT 'event_logs', COUNT(*), MAX(loaded_at) FROM raw.event_logs
UNION ALL
SELECT 'user_sessions', COUNT(*), MAX(loaded_at) FROM raw.user_sessions
UNION ALL
SELECT 'moderation_queue', COUNT(*), MAX(loaded_at) FROM raw.moderation_queue
UNION ALL
SELECT 'user_recommendations', COUNT(*), MAX(loaded_at) FROM raw.user_recommendations;

-- Даем права
GRANT ALL PRIVILEGES ON SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO airflow;

-- Проверяем созданные таблицы
\dt raw.*;