import os
import pandas as pd

# Пути к папкам
input_folder = 'data/excel_files'
csv_folder = 'data/output_csv'

def convert(input_folder, csv_folder):
    # Создаем папку для вывода, если она не существует
    os.makedirs(csv_folder, exist_ok=True)

    # Проходим по всем файлам в папке input_folder
    for filename in os.listdir(input_folder):
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            # Полный путь к Excel файлу
            excel_file = os.path.join(input_folder, filename)

            # Загрузка Excel файла
            xls = pd.ExcelFile(excel_file)

            # Конвертация каждого листа в отдельный CSV файл
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                csv_filename = f'{os.path.splitext(filename)[0]}_{sheet_name}.csv'
                csv_file = os.path.join(csv_folder, csv_filename)
                df.to_csv(csv_file, index=False, sep='$')
                print(f'Лист {sheet_name} из файла {filename} успешно конвертирован в {csv_file}')

def validate(csv_folder):
    pass

if __name__ == '__main__':
    convert(input_folder, csv_folder)