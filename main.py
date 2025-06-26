import uuid
import os
import asyncio
import time
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from file_processing import process_xlsb_file
import pandas as pd
from typing import Dict
from contextlib import asynccontextmanager

app = FastAPI(title="XLSX Processing API")

# Словарь для хранения статуса задач
tasks: Dict[str, dict] = {}


# Модель для ответа статуса
class TaskStatus(BaseModel):
    """Модель для ответа со статусом задачи."""
    task_id: str
    status: str
    error: str | None = None


# Создаем директорию для временных файлов
TEMP_DIR = "temp"
os.makedirs(TEMP_DIR, exist_ok=True)

# Максимальный возраст файлов (1 час)
MAX_FILE_AGE = 3600  # в секундах


def cleanup_old_files():
    """Удаляет старые файлы из TEMP_DIR."""
    now = time.time()
    for filename in os.listdir(TEMP_DIR):
        file_path = os.path.join(TEMP_DIR, filename)
        if os.path.isfile(file_path) and (
                now - os.path.getmtime(file_path)) > MAX_FILE_AGE:
            os.remove(file_path)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Обработчик жизненного цикла приложения."""
    cleanup_old_files()
    yield


# Привязываем lifespan к приложению
app = FastAPI(title="XLSX Processing API", lifespan=lifespan)


@app.post("/upload", response_model=dict)
async def upload_file(file: UploadFile = File(...)):
    """Принимает XLSX-файл, запускает асинхронную обработку и возвращает task_id."""
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400,
                            detail="Требуется файл с расширением .xlsx")

    # Генерируем уникальный task_id
    task_id = str(uuid.uuid4())

    # Путь для сохранения входного и выходного файлов
    input_path = os.path.join(TEMP_DIR, f"{task_id}_input.xlsx")
    output_path = os.path.join(TEMP_DIR, f"{task_id}_output.xlsx")

    # Инициализируем статус задачи
    tasks[task_id] = {"status": "pending", "output_path": output_path}

    # Сохраняем загруженный файл
    content = await file.read()
    with open(input_path, "wb") as f:
        f.write(content)

    # Проверка формата
    try:
        pd.read_excel(input_path)
    except ValueError:
        os.remove(input_path)
        raise HTTPException(status_code=400,
                            detail="Некорректный формат XLSX-файла")

    # Асинхронная функция для обработки файла
    async def process_task():
        try:
            process_xlsb_file(input_path, output_path)
            tasks[task_id]["status"] = "success"
        except Exception as e:
            tasks[task_id]["status"] = "failed"
            tasks[task_id]["error"] = str(e)
        finally:
            # Удаляем входной файл после обработки
            if os.path.exists(input_path):
                os.remove(input_path)

    # Запускаем задачу асинхронно
    asyncio.create_task(process_task())
    return {"task_id": task_id}


@app.get("/status/{task_id}", response_model=TaskStatus)
async def get_status(task_id: str):
    """Возвращает статус задачи по task_id."""
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Задача не найдена")

    return TaskStatus(
        task_id=task_id,
        status=tasks[task_id]["status"],
        error=tasks[task_id].get("error")
    )


@app.get("/result/{task_id}", response_class=FileResponse)
async def get_result(task_id: str):
    """Возвращает обработанный XLSX-файл, если задача завершена."""
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Задача не найдена")

    if tasks[task_id]["status"] != "success":
        raise HTTPException(status_code=404,
                            detail="Задача не завершена или завершилась с ошибкой")

    output_path = tasks[task_id]["output_path"]
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Результат не найден")

    return FileResponse(
        path=output_path,
        filename=f"result_{task_id}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
