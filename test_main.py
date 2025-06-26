import unittest
import pandas as pd
import os
import time
import warnings
from fastapi.testclient import TestClient
from unittest.mock import patch
from main import app, tasks, TEMP_DIR


class TestXLSXProcessingAPI(unittest.TestCase):
    """Тесты для API обработки XLSX-файлов."""

    def setUp(self):
        """Подготовка тестовых данных и клиента."""
        # Игнорируем предупреждения openpyxl и pandas
        warnings.filterwarnings("ignore", category=RuntimeWarning,
                                module="openpyxl")
        warnings.filterwarnings("ignore", category=RuntimeWarning,
                                module="pandas")
        # Игнорируем предупреждение о необработанной коррутине
        warnings.filterwarnings("ignore", category=RuntimeWarning,
                                message=".*coroutine.*was never awaited.*")

        self.client = TestClient(app)
        self.test_data = pd.DataFrame({
            'Поступило всего': [506.0, 369.0, 723.0],
            '№ Акта ВК': ['', '', ''],
            'Дата выпуска акта ВК': ['', '', ''],
            '№ упаковочного листа': ['', '', ''],
            'ID Материала': ['I46872', '694304', '727698'],
            'Наименование ТМЦ': ['Бетонная смесь B25', 'Бетонная смесь B25',
                                 'Гайка М20'],
            'Единица измерения': ['м3', 'м3', 'кг'],
            'Кол-во по заявке': ['358 М3', '80 М3', '934 КГ'],
            'Масса, кг': ['-', '-', '-'],
            'Длина, м': ['-', '-', '-'],
            'Площадь, м²': ['-', '-', '-'],
            'Объем, м³': ['-', '-', '-']
        })
        self.test_file_path = os.path.join(TEMP_DIR, "test_input.xlsx")
        self.test_data.to_excel(self.test_file_path, index=False)
        self.invalid_file_path = os.path.join(TEMP_DIR, "invalid.txt")
        with open(self.invalid_file_path, "w") as f:
            f.write("This is not an XLSX file")

    def tearDown(self):
        """Очистка после тестов."""
        # Удаляем тестовые файлы
        for file in [self.test_file_path, self.invalid_file_path]:
            if os.path.exists(file):
                os.remove(file)
        # Очищаем словарь задач
        tasks.clear()
        # Удаляем временные файлы в TEMP_DIR
        for file in os.listdir(TEMP_DIR):
            file_path = os.path.join(TEMP_DIR, file)
            if os.path.isfile(file_path):
                os.remove(file_path)

    @patch("main.asyncio.create_task")
    def test_upload_valid_file(self, mock_create_task):
        """Тест загрузки корректного XLSX-файла."""
        mock_create_task.return_value = None  # Задача остается pending
        with open(self.test_file_path, "rb") as f:
            response = self.client.post("/upload", files={"file": (
                "test.xlsx", f,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertIn("task_id", response_json)
        task_id = response_json["task_id"]
        self.assertTrue(task_id in tasks)
        self.assertEqual(tasks[task_id]["status"], "pending")

    def test_upload_invalid_file(self):
        """Тест загрузки файла с неверным расширением."""
        with open(self.invalid_file_path, "rb") as f:
            response = self.client.post("/upload", files={
                "file": ("invalid.txt", f, "text/plain")})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"],
                         "Требуется файл с расширением .xlsx")

    @patch("main.asyncio.create_task")
    def test_status_pending(self, mock_create_task):
        """Тест получения статуса задачи в состоянии pending."""
        mock_create_task.return_value = None  # Задача остается pending
        with open(self.test_file_path, "rb") as f:
            response = self.client.post("/upload", files={"file": (
                "test.xlsx", f,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        self.assertEqual(response.status_code, 200)
        task_id = response.json()["task_id"]

        response = self.client.get(f"/status/{task_id}")
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json["task_id"], task_id)
        self.assertEqual(response_json["status"], "pending")
        self.assertIsNone(response_json["error"])

    def test_status_not_found(self):
        """Тест получения статуса несуществующей задачи."""
        response = self.client.get(
            "/status/123e4567-e89b-12d3-a456-426614174000")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"], "Задача не найдена")

    def test_result_not_found(self):
        """Тест получения результата для несуществующей задачи."""
        response = self.client.get(
            "/result/123e4567-e89b-12d3-a456-426614174000")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"], "Задача не найдена")

    @patch("main.asyncio.create_task")
    def test_result_pending(self, mock_create_task):
        """Тест получения результата для незавершенной задачи."""
        mock_create_task.return_value = None  # Задача остается pending
        with open(self.test_file_path, "rb") as f:
            response = self.client.post("/upload", files={"file": (
                "test.xlsx", f,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        self.assertEqual(response.status_code, 200)
        task_id = response.json()["task_id"]

        response = self.client.get(f"/result/{task_id}")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"],
                         "Задача не завершена или завершилась с ошибкой")

    def test_result_success(self):
        """Тест получения результата после успешного завершения задачи."""
        with open(self.test_file_path, "rb") as f:
            response = self.client.post("/upload", files={"file": (
                "test.xlsx", f,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        self.assertEqual(response.status_code, 200)
        task_id = response.json()["task_id"]

        # Ждем завершения задачи
        for _ in range(10):  # Максимум 10 секунд
            response = self.client.get(f"/status/{task_id}")
            if response.json()["status"] == "success":
                break
            time.sleep(1)

        response = self.client.get(f"/result/{task_id}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"],
                         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    @patch("main.process_xlsb_file", side_effect=Exception("Ошибка обработки"))
    def test_task_failed(self, mock_process):
        """Тест обработки задачи с ошибкой."""
        with open(self.test_file_path, "rb") as f:
            response = self.client.post("/upload", files={"file": (
                "test.xlsx", f,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        self.assertEqual(response.status_code, 200)
        task_id = response.json()["task_id"]

        # Ждем завершения задачи
        for _ in range(10):
            response = self.client.get(f"/status/{task_id}")
            if response.json()["status"] == "failed":
                break
            time.sleep(1)

        response = self.client.get(f"/status/{task_id}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "failed")
        self.assertEqual(response.json()["error"], "Ошибка обработки")

        response = self.client.get(f"/result/{task_id}")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"],
                         "Задача не завершена или завершилась с ошибкой")


if __name__ == '__main__':
    unittest.main()
