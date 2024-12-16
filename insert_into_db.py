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
    df['Фармако-терапевтическая группа'] = df['Фармако-терапевтическая группа'].str.replace(r'[^A-Za-zА-Яа-я0-9 +]', '',
                                                                                            regex=True)
    df['Торговое наименование'] = df['Торговое наименование'].str.replace(r'[^A-Za-zА-Яа-я0-9 ]', '', regex=True)
    df['Наименование держателя'] = df['Наименование держателя'].str.replace(r'[^A-Za-zА-Яа-я0-9 ]', '', regex=True)
    df['Страна держателя'] = df['Страна держателя'].str.replace(r'[^A-Za-zА-Яа-я0-9 ]', '', regex=True)
    # Срок годности не нуждается в постобработке
    # Условия хранения не нуждаются в постобработке
    df['Количество таблеток'] = df['Количество таблеток'].str.replace(r'[^0-9+]', '', regex=True)
    df1 = prepare_release_form(df['Торговое наименование'], df['Дозировка'], df['Количество таблеток'])
    df['Фармако-терапевтическая группа'] = df['Фармако-терапевтическая группа'].str.replace(r'[^A-Za-zА-Яа-я0-9 -+]',
                                                                                            '', regex=True)
    # Поля Рецептурное, БАД, Файл с инструкцией не нуждаются в постобработке
    return df, df1


sql_script_pharmacological_groups = []
sql_script_legal_entities = []
sql_script_release_forms = []
sql_script_medications = []
sql_script_medication_release_forms = []
sql_script_medication_legal_entities = []
sql_script_medication_pharmacological_groups = []
sql_script_instructions = []

# Генерация SQL запросов для каждой таблицы
for filename in os.listdir(csv_folder):
    if filename.endswith('.csv'):
        file_path = os.path.join(csv_folder, filename)
        df = pd.read_csv(file_path, sep=',', encoding='utf-8')

        # Очистка данных
        df, df1 = preparation_data(df)

        # Генерация строк данных для вставки в таблицу pharmacological_groups
        pharmacological_groups = df[['Фармако-терапевтическая группа']].drop_duplicates().rename(
            columns={'Фармако-терапевтическая группа': 'name'})

        # Новый список для хранения результатов
        pharmacological_groups_F = []
        for item in pharmacological_groups['name']:
            if '+' in item:
                pharmacological_groups_F.extend(item.split('+'))
            else:
                pharmacological_groups_F.append(item)

        # Добавляем запросы для pharmacological_groups
        for name in pharmacological_groups_F:
            sql_script_pharmacological_groups.append(
                f"INSERT INTO core.pharmacological_groups (name) VALUES ('{name.lower()}') ON CONFLICT (name) DO NOTHING;")

        # Генерация строк данных для вставки в таблицу legal_entities
        legal_entities = df[['Наименование держателя', 'Страна держателя']].drop_duplicates().rename(
            columns={'Наименование держателя': 'name', 'Страна держателя': 'country'})
        for index, row in legal_entities.iterrows():
            sql_script_legal_entities.append(
                f"INSERT INTO core.legal_entities (name, country) VALUES ('{row['name']}', '{row['country']}') ON CONFLICT (name, country) DO NOTHING;")

        # Генерация строк данных для вставки в таблицу release_forms
        for row in df1:
            sql_script_release_forms.append(
                f"INSERT INTO core.release_forms (dosage_per_tablet, tablets_count) VALUES ('{row[1]}', {row[2]}) ON CONFLICT (dosage_per_tablet, tablets_count) DO NOTHING;")

        # Генерация строк данных для вставки в таблицу medications
        for index, row in df.iterrows():
            legal_entity_id = f"(SELECT id FROM core.legal_entities WHERE name = '{row['Наименование держателя']}' AND country = '{df.loc[index, 'Страна держателя']}')"
            pharmacological_group_id = f"(SELECT id FROM core.pharmacological_groups WHERE name = '{row['Фармако-терапевтическая группа']}')"
            sql_script_medications.append(
                f"""
INSERT INTO core.medications (trade_name, storage_conditions, is_prescription, is_dietary_supplement)
VALUES ('{row['Торговое наименование']}', '{row['Условия хранения']}', {row['Рецептурное']}, false)
ON CONFLICT (trade_name) DO NOTHING;
""")

        # Генерация строк для связывания medications и legal_entities
        legal_entities_with_medicine = df[
            ['Наименование держателя', 'Страна держателя', 'Торговое наименование']].rename(
            columns={'Наименование держателя': 'name', 'Страна держателя': 'country',
                     'Торговое наименование': 'trade_name'})
        for index, row in legal_entities_with_medicine.iterrows():
            legal_entities_id = f"""
(SELECT id FROM core.legal_entities
WHERE name = '{row['name']}' AND country = '{row['country']}')
"""
            medication_id = f"""
(SELECT id FROM core.medications
WHERE trade_name = '{row['trade_name']}')
"""
            sql_script_medication_legal_entities.append(
                f"""
INSERT INTO core.medication_legal_entities (medication_id, legal_entity_id)
VALUES ({medication_id}, {legal_entities_id})
ON CONFLICT (medication_id, legal_entity_id) DO NOTHING;
"""
            )

        # Генерация строк для связывания medications и pharmacological_groups
        pharmacological_groups = df[['Фармако-терапевтическая группа', 'Торговое наименование']].rename(
            columns={'Фармако-терапевтическая группа': 'name', 'Торговое наименование': 'trade_name'})

        # Новый список для хранения результатов
        pharmacological_groups_F_with_medicine = []
        for index, row in pharmacological_groups.iterrows():
            # Разделение по "+" в фармако-группах
            if '+' in row['name']:
                groups = row['name'].split('+')
                for group in groups:
                    pharmacological_groups_F_with_medicine.append((group.strip(), row['trade_name']))
            else:
                pharmacological_groups_F_with_medicine.append((row['name'], row['trade_name']))

        # Новый список для SQL-запросов
        sql_script_medication_pharmacological_groups = []
        for group, trade_name in pharmacological_groups_F_with_medicine:
            pharmacological_groups_id = f"""
(SELECT id FROM core.pharmacological_groups
WHERE name = '{group.lower()}')
"""
            medication_id = f"""
(SELECT id FROM core.medications
WHERE trade_name = '{trade_name}')
"""
            sql_script_medication_pharmacological_groups.append(
                f"""
INSERT INTO core.medication_pharmacological_groups (medication_id, pharmacological_group_id)
VALUES ({medication_id}, {pharmacological_groups_id})
ON CONFLICT (medication_id, pharmacological_group_id) DO NOTHING;
"""
            )

        # Генерация строк для связывания medications и release_forms
        for row in df1:
            release_form_id = f"""
(SELECT id FROM core.release_forms
WHERE dosage_per_tablet = '{row[1]}' AND tablets_count = {row[2]})
"""
            medication_id = f"""
(SELECT id FROM core.medications
WHERE trade_name = '{row[0]}')
"""
            sql_script_medication_release_forms.append(
                f"""
INSERT INTO core.medication_release_forms (medication_id, release_form_id)
VALUES ({medication_id}, {release_form_id})
ON CONFLICT (medication_id, release_form_id) DO NOTHING;
"""
            )

        # Генерация инструкций
        for index, row in df.iterrows():
            instruction_text = extract_text_from_pdf(row['Файл с инструкцией'])
            medication_id = f"""
(SELECT id FROM core.medications
WHERE trade_name = '{row['Торговое наименование']}')
"""
            sql_script_instructions.append(
                f"""
INSERT INTO core.instructions (medication_id, content)
VALUES ({medication_id}, '{instruction_text.replace("'", "")}');
"""
            )

# Сохранение SQL-скриптов в разные файлы
with open('data/generated_sql_pharmacological_groups.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_script_pharmacological_groups))

with open('data/generated_sql_legal_entities.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_script_legal_entities))

with open('data/generated_sql_release_forms.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_script_release_forms))

with open('data/generated_sql_medications.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_script_medications))

with open('data/generated_sql_medication_legal_entities.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_script_medication_legal_entities))

with open('data/generated_sql_medication_pharmacological_groups.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_script_medication_pharmacological_groups))

with open('data/generated_sql_medication_release_forms.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_script_medication_release_forms))

with open('data/generated_sql_instructions.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_script_instructions))

with open('data/generated_sql.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(sql_script_pharmacological_groups))
    f.write('\n'.join(sql_script_legal_entities))
    f.write('\n'.join(sql_script_release_forms))
    f.write('\n'.join(sql_script_medications))
    f.write('\n'.join(sql_script_medication_legal_entities))
    f.write('\n'.join(sql_script_medication_pharmacological_groups))
    f.write('\n'.join(sql_script_medication_release_forms))
    f.write('\n'.join(sql_script_instructions))
