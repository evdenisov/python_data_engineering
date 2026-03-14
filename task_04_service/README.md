# Energy Dashboard Service

Автор: ДЕНИСОВ ЕВГЕНИЙ ВИКТОРОВИЧ

Веб-сервис для визуализации и управления данными об энергопотреблении. Состоит из двух частей:
- **Backend**: FastAPI (порт 8000) - обработка данных и API
- **Frontend**: Streamlit (порт 8501) - пользовательский интерфейс

## Клонируйте репозиторий
git clone https://github.com/evdenisov/python_data_engineering

## Перейдите в папку проекта
cd python_data_engineering/task_04_service

## В терминале выполните
docker-compose up --build

## Доступ к сервисам
После успешного запуска откройте в браузере:
Frontend (интерфейс): http://localhost:8501
Backend API: http://localhost:8000
Документация API: http://localhost:8000/docs

## Остановка сервиса 
docker-compose down

## Структура проекта 
task_04_service/
├── backend/
│   ├── main.py              # FastAPI приложение
│   ├── data.csv              # Данные (ваш файл)
│   └── requirements.txt      # Зависимости для backend
├── frontend/
│   ├── app.py                # Streamlit приложение
│   └── requirements.txt      # Зависимости для frontend
├── docker-compose.yml        # Оркестрация контейнеров
├── Dockerfile.backend        # Dockerfile для backend
├── Dockerfile.frontend       # Dockerfile для frontend
└── README.md                 # Этот файл