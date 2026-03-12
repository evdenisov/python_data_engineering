from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import pandas as pd
import os
from typing import List, Optional
import logging
import traceback

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Energy Dashboard API")

# Путь к файлу с данными
DATA_FILE = "data.csv"

# Модели Pydantic для валидации
class EnergyRecord(BaseModel):
    id: Optional[int] = None
    timestamp: str
    consumption_europe: float
    consumption_asia: float
    price_europe: float
    price_asia: float

    class Config:
        from_attributes = True

class EnergyRecordCreate(BaseModel):
    timestamp: str
    consumption_europe: float
    consumption_asia: float
    price_europe: float
    price_asia: float

# Функция для загрузки данных из CSV
def load_data():
    try:
        if os.path.exists(DATA_FILE):
            logger.info(f"Loading data from {DATA_FILE}")
            
            # Читаем файл (у вашего CSV уже правильные названия колонок!)
            df = pd.read_csv(DATA_FILE)
            logger.info(f"Data loaded: {len(df)} rows")
            logger.info(f"Columns in CSV: {list(df.columns)}")
            
            # Добавляем колонку id если её нет
            if 'id' not in df.columns:
                df.insert(0, 'id', range(1, len(df) + 1))
            
            return df
        else:
            logger.warning(f"File {DATA_FILE} not found, creating empty dataframe")
            # Создаем пустой DataFrame с правильными колонками
            return pd.DataFrame(columns=['id', 'timestamp', 'consumption_europe', 
                                        'consumption_asia', 'price_europe', 'price_asia'])
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['id', 'timestamp', 'consumption_europe', 
                                    'consumption_asia', 'price_europe', 'price_asia'])

# Функция для сохранения данных в CSV
def save_data(df):
    try:
        logger.info(f"Saving data to {DATA_FILE}")
        
        # Создаем копию для сохранения
        save_df = df.copy()
        
        # Убираем колонку id при сохранении
        if 'id' in save_df.columns:
            save_df = save_df.drop('id', axis=1)
        
        save_df.to_csv(DATA_FILE, index=False)
        logger.info(f"Data saved: {len(save_df)} rows")
    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error saving data: {str(e)}")

@app.get("/records", response_model=List[EnergyRecord])
async def get_records():
    """Получить все записи"""
    try:
        logger.info("GET /records called")
        df = load_data()
        
        if df.empty:
            logger.info("No records found, returning empty list")
            return []
        
        # Преобразуем DataFrame в список словарей
        records = df.to_dict('records')
        logger.info(f"Returning {len(records)} records")
        
        return records
    except Exception as e:
        logger.error(f"Error in get_records: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/records", response_model=EnergyRecord, status_code=status.HTTP_201_CREATED)
async def create_record(record: EnergyRecordCreate):
    """Добавить новую запись"""
    try:
        logger.info(f"POST /records called")
        df = load_data()
        
        # Создаем новую запись с id
        new_id = 1
        if not df.empty and 'id' in df.columns:
            new_id = int(df['id'].max()) + 1
        elif not df.empty:
            new_id = len(df) + 1
        
        new_record = record.dict()
        new_record['id'] = new_id
        
        # Добавляем в DataFrame
        new_df = pd.DataFrame([new_record])
        df = pd.concat([df, new_df], ignore_index=True)
        
        # Сохраняем
        save_data(df)
        
        logger.info(f"Record created with id: {new_id}")
        return new_record
    except Exception as e:
        logger.error(f"Error in create_record: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/records/{record_id}")
async def delete_record(record_id: int):
    """Удалить запись по id"""
    try:
        logger.info(f"DELETE /records/{record_id} called")
        df = load_data()
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"Record with id {record_id} not found")
        
        if 'id' not in df.columns:
            raise HTTPException(status_code=500, detail="Data format error: id column missing")
        
        if record_id not in df['id'].values:
            raise HTTPException(status_code=404, detail=f"Record with id {record_id} not found")
        
        # Удаляем запись
        df = df[df['id'] != record_id]
        
        # Сохраняем
        save_data(df)
        
        logger.info(f"Record {record_id} deleted successfully")
        return {"message": f"Record {record_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in delete_record: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/debug")
async def debug_info():
    """Отладочная информация"""
    try:
        df = load_data()
        return {
            "file_exists": os.path.exists(DATA_FILE),
            "file_size": os.path.getsize(DATA_FILE) if os.path.exists(DATA_FILE) else 0,
            "rows": len(df),
            "columns": list(df.columns) if not df.empty else [],
            "sample": df.head(3).to_dict('records') if not df.empty else []
        }
    except Exception as e:
        return {"error": str(e)}