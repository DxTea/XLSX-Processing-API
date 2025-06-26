import pandas as pd


def normalize_numeric(value):
    """Преобразует значение в число, убирая единицы измерения и заменяя запятую на точку."""
    if pd.isna(value):
        return value
    value = str(value).replace(',', '.')  # Заменяем запятую на точку
    # Удаляем текстовые единицы измерения
    for unit in ['М3', 'КГ', 'Т', 'шт', 'кг', 'т', 'м3']:
        value = value.replace(unit, '').strip()
    try:
        return float(value)
    except ValueError:
        return value


def process_xlsb_file(input_path, output_path):
    """Обрабатывает XLSX-файл и сохраняет результат в XLSX."""
    try:
        # Чтение файла XLSX
        df = pd.read_excel(input_path)
        if df.empty:
            raise ValueError("Файл пустой")

        # Проверка наличия необходимых колонок
        required_columns = ['ID Материала', 'Поступило всего',
                            'Кол-во по заявке']
        missing_columns = [col for col in required_columns if
                           col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"Отсутствуют колонки: {', '.join(missing_columns)}")

        # Нормализация данных
        df['ID Материала'] = df['ID Материала'].apply(
            lambda x: str(x).replace('I', '1'))
        df['Поступило всего'] = df['Поступило всего'].apply(normalize_numeric)
        df['Кол-во по заявке'] = df['Кол-во по заявке'].apply(
            normalize_numeric)

        # Приведение к числовому типу
        df['Поступило всего'] = pd.to_numeric(df['Поступило всего'],
                                              errors='coerce')
        df['Кол-во по заявке'] = pd.to_numeric(df['Кол-во по заявке'],
                                               errors='coerce')

        # Проверка на NaN после преобразования
        if df[['Поступило всего', 'Кол-во по заявке']].isna().any().any():
            raise ValueError("В числовых колонках есть некорректные данные")

        # Фильтрация
        filtered_df = df[df['Кол-во по заявке'] > df['Поступило всего']].copy()

        # Добавление колонки
        filtered_df.loc[:, 'Расхождение заявка-приход'] = (
                filtered_df['Кол-во по заявке'] - filtered_df[
            'Поступило всего']
        )

        # Сохранение результата
        filtered_df.to_excel(output_path, index=False)
        print(f"Результат сохранен в {output_path}")
    except Exception as e:
        raise Exception(f"Ошибка обработки файла: {str(e)}")
