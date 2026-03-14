# Energy Dashboard Service

Автор: ДЕНИСОВ ЕВГЕНИЙ ВИКТОРОВИЧ

Веб-сервис для визуализации и управления данными об энергопотреблении. Состоит из двух частей:
- **Backend**: FastAPI (порт 8000) - обработка данных и API
- **Frontend**: Streamlit (порт 8501) - пользовательский интерфейс

## 1.Установка и запуск

### Клонируйте репозиторий
git clone https://github.com/evdenisov/python_data_engineering

### Перейдите в папку проекта
cd python_data_engineering/task_04_service

### В терминале выполните
docker-compose up --build

### Доступ к сервисам
После успешного запуска откройте в браузере:
Frontend (интерфейс): http://localhost:8501
Backend API: http://localhost:8000
Документация API: http://localhost:8000/docs

### Остановка сервиса 
docker-compose down

## 2. Использование приложения

### Просмотр данных
- На главной странице отображается таблица со всеми записями
- Автоматически строятся два графика:
  - Потребление энергии по регионам
  - Цены по регионам

### Добавление записи
1. В правой панели найдите форму "Add New Record"
2. Заполните поля и нажмите "Add Record"
3. Таблица и графики обновятся автоматически

### Удаление записи
1. В нижней части страницы введите ID записи
2. Нажмите кнопку "Delete"
3. Данные обновятся автоматически

## 3. Структура проекта

```
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
```

## 4. Формат данных
Файл `backend/data.csv` должен иметь структуру:
```csv
timestamp,consumption_europe,consumption_asia,price_europe,price_asia
2006-09-01 00:00,62341,17916,275.22,0.00
2006-09-01 01:00,60625,17467,0.00,0.00
...
```

## 5. Проверка работоспособности
- **Frontend**: http://localhost:8501 - должен открываться интерфейс
- **API**: http://localhost:8000/records - должен показывать JSON с данными
- **Документация**: http://localhost:8000/docs - должна открываться Swagger UI