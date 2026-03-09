FROM apache/airflow:2.7.0

USER root

# Установите системные зависимости
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Копируем requirements.txt
COPY requirements.txt /requirements.txt

# Функция для установки с повторными попытками
RUN pip install --upgrade pip && \
    pip config set global.timeout 120 && \
    for i in 1 2 3; do \
        pip install --no-cache-dir --default-timeout=120 -r /requirements.txt && break || \
        if [ $i -eq 3 ]; then exit 1; else sleep 10; fi; \
    done