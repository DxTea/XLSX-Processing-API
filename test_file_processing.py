import unittest
import pandas as pd
import os
from file_processing import normalize_numeric, process_xlsb_file


class MockCell:
    """Класс для имитации ячейки XLSB с атрибутом .v."""

    def __init__(self, value):
        self.v = value


class TestFileProcessing(unittest.TestCase):
    """Тесты для обработки XLSB-файла."""

    def setUp(self):
        """Подготовка тестовых данных."""
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
        self.input_file = 'test_input.xlsx'
        self.output_file = 'test_output.xlsx'

    def test_normalize_numeric(self):
        """Тест нормализации числовых значений."""
        self.assertEqual(normalize_numeric('80 М3'), 80.0)
        self.assertEqual(normalize_numeric('934 КГ'), 934.0)
        self.assertEqual(normalize_numeric('358'), 358.0)
        self.assertEqual(normalize_numeric('invalid'), 'invalid')
        self.assertTrue(pd.isna(normalize_numeric(None)))

    def test_normalize_id_material(self):
        """Тест замены 'I' на '1' в ID Материала."""
        df = self.test_data.copy()
        df['ID Материала'] = df['ID Материала'].apply(
            lambda x: str(x).replace('I', '1'))
        self.assertEqual(df['ID Материала'].iloc[0], '146872')

    def test_filtering(self):
        """Тест фильтрации строк, где Кол-во по заявке > Поступило всего."""
        df = self.test_data.copy()
        df['Поступило всего'] = df['Поступило всего'].apply(normalize_numeric)
        df['Кол-во по заявке'] = df['Кол-во по заявке'].apply(
            normalize_numeric)
        df['Поступило всего'] = pd.to_numeric(df['Поступило всего'],
                                              errors='coerce')
        df['Кол-во по заявке'] = pd.to_numeric(df['Кол-во по заявке'],
                                               errors='coerce')
        filtered_df = df[df['Кол-во по заявке'] > df['Поступило всего']].copy()
        self.assertEqual(len(filtered_df), 1)  # Только одна строка: 934 > 723
        self.assertEqual(filtered_df['Наименование ТМЦ'].iloc[0], 'Гайка М20')

    def test_add_discrepancy_column(self):
        """Тест добавления колонки Расхождение заявка-приход."""
        df = self.test_data.copy()
        df['Поступило всего'] = df['Поступило всего'].apply(normalize_numeric)
        df['Кол-во по заявке'] = df['Кол-во по заявке'].apply(
            normalize_numeric)
        df['Поступило всего'] = pd.to_numeric(df['Поступило всего'],
                                              errors='coerce')
        df['Кол-во по заявке'] = pd.to_numeric(df['Кол-во по заявке'],
                                               errors='coerce')
        filtered_df = df[df['Кол-во по заявке'] > df['Поступило всего']].copy()
        filtered_df.loc[:, 'Расхождение заявка-приход'] = filtered_df[
                                                              'Кол-во по заявке'] - \
                                                          filtered_df[
                                                              'Поступило всего']
        self.assertIn('Расхождение заявка-приход', filtered_df.columns)
        self.assertEqual(filtered_df['Расхождение заявка-приход'].iloc[0],
                         934.0 - 723.0)

    def test_process_invalid_numeric_data(self):
        """Тест обработки файла с некорректными числовыми данными."""
        invalid_data = pd.DataFrame({
            'ID Материала': ['I123'],
            'Поступило всего': ['abc'],  # Некорректное значение
            'Кол-во по заявке': ['100']
        })
        invalid_input = 'test_invalid_input.xlsx'
        invalid_data.to_excel(invalid_input, index=False)

        with self.assertRaises(Exception) as context:
            process_xlsb_file(invalid_input, 'test_invalid_output.xlsx')
        self.assertIn("некорректные данные", str(context.exception))

        os.remove(invalid_input)


if __name__ == '__main__':
    unittest.main()
