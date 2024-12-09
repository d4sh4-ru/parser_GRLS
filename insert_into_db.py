import os
import pandas as pd
import fitz

# Путь к файлу CSV
csv_folder = 'data/'

def extract_text_from_pdf(pdf_path: str) -> str:
    """Функция для извлечения текста из PDF."""
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc:
            text += page.get_text("text")
        return text.strip()
    except Exception as e:
        print(f"Ошибка при извлечении текста из {pdf_path}: {e}")
        return ""

def prepare_release_form(trade_names: list, dosage_list: list, tablets_count_list: list) -> list[list[str]]:
    result = []
    # Проходим по парам элементов из обоих списков
    for trade_name, dosage, tablets_count in zip(trade_names, dosage_list, tablets_count_list):
        # Разбиваем строки на числа
        try:
            nums_dosage = map(str, dosage.split('+'))
            nums_tablets_count = map(str, tablets_count.split('+'))
        except AttributeError:
            print(f"Ошибка в строке {trade_name} {dosage}, {tablets_count}")
            continue
        # Комбинируем числа в парах
        result.extend([[trade_name, x, y] for x in nums_dosage for y in nums_tablets_count])
    return result

# Функция для очистки данных
def preparation_data(df: pd.DataFrame) -> tuple[pd.DataFrame, list[list[str]]]:
    df['Торговое наименование'] = df['Торговое наименование'].str.replace(r'[^A-Za-zА-Яа-я0-9 ]', '', regex=True)

    print(df['Торговое наименование'])

    df['Наименование держателя'] = df['Наименование держателя'].str.replace(r'[^A-Za-zА-Яа-я0-9 ]', '', regex=True)
    df['Страна держателя'] = df['Страна держателя'].str.replace(r'[^A-Za-zА-Яа-я0-9 ]', '', regex=True)

    print(df['Наименование держателя'])
    print(df['Страна держателя'])
    # Срок годности не нуждается в постобработке
    # Условия хранения не нуждаются в постобработке
    df['Количество таблеток'] = df['Количество таблеток'].str.replace(r'[^0-9+]', '', regex=True)

    print(df['Количество таблеток'])

    df1 = prepare_release_form(df['Торговое наименование'], df['Дозировка'], df['Количество таблеток'])
    print(df1)
    df['Фармако-терапевтическая группа'] = df['Фармако-терапевтическая группа'].str.replace(r'[^A-Za-zА-Яа-я0-9 -+]', '', regex=True)
    print(df['Фармако-терапевтическая группа'])
    # Поля Рецептурное, БАД, Файл с инструкцией не нуждаются в постобработке
    return df, df1

# Чтение CSV файлов и генерация SQL-скрипта
sql_script = []

for filename in os.listdir(csv_folder):
    if filename.endswith('.csv'):
        file_path = os.path.join(csv_folder, filename)
        df = pd.read_csv(file_path, sep=',', encoding='utf-8')

        print(df)

        # Очистка данных
        df, df1 = preparation_data(df)

        # Генерация строк данных для вставки в таблицу PharmacologicalGroups
        pharmacological_groups = df[['Фармако-терапевтическая группа']].drop_duplicates().rename(columns={'Фармако-терапевтическая группа': 'name'})
        for name in pharmacological_groups['name']:
            sql_script.append(f"INSERT INTO core.PharmacologicalGroups (name) VALUES ('{name}') ON CONFLICT (name) DO NOTHING;")

        # Генерация строк данных для вставки в таблицу LegalEntities
        legal_entities = df[['Наименование держателя', 'Страна держателя']].drop_duplicates().rename(columns={'Наименование держателя': 'name', 'Страна держателя': 'country'})
        for index, row in legal_entities.iterrows():
            sql_script.append(f"INSERT INTO core.LegalEntities (name, country) VALUES ('{row['name']}', '{row['country']}') ON CONFLICT (name, country) DO NOTHING;")

        # Генерация строк данных для вставки в таблицу ReleaseForms
        for row in df1:
            sql_script.append(f"INSERT INTO core.ReleaseForms (dosage_per_tablet, tablets_count) VALUES ('{row[1]}', {row[2]}) ON CONFLICT (dosage_per_tablet, tablets_count) DO NOTHING;")


        for index, row in df.iterrows():
            legal_entity_id = f"(SELECT id FROM core.LegalEntities WHERE name = '{row['Наименование держателя']}' AND country = '{df.loc[index, 'Страна держателя']}')"
            pharmacological_group_id = f"(SELECT id FROM core.PharmacologicalGroups WHERE name = '{row['Фармако-терапевтическая группа']}')"

            sql_script.append(
f"""
INSERT INTO core.Medications (trade_name, legal_entity_id, pharmacological_group_id, is_prescription, is_dietary_supplement)
VALUES ('{row['Торговое наименование']}', {legal_entity_id}, {pharmacological_group_id}, {row['Рецептурное']}, false)
ON CONFLICT (trade_name) DO NOTHING;
""")

        for row in df1:
            release_form_id = f"""
                        (SELECT id FROM core.ReleaseForms
                        WHERE dosage_per_tablet = '{row[1]}' AND tablets_count = {row[2]})
                    """
            sql_script.append(
f"""
INSERT INTO core.MedicationReleaseForms (medication_id, release_form_id)
VALUES ('{row[0]}', {release_form_id})
ON CONFLICT (medication_id, release_form_id) DO NOTHING;
"""
            )

        for index, row in df.iterrows():
            instruction_text = extract_text_from_pdf(row['Файл с инструкцией'])
            sql_script.append(
f"""
INSERT INTO core.Instructions (medication_trade_name, content)
VALUES ('{row['Торговое наименование']}', '{instruction_text}')
ON CONFLICT (medication_trade_name, content) DO NOTHING;
"""
            )

# Сохранение SQL-скрипта в файл
with open('data/generated_sql_script.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_script))

print("SQL-скрипт успешно сгенерирован и сохранен в файл 'data/generated_sql_script.sql'")
