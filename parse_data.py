import csv
import requests
from bs4 import BeautifulSoup
import re
import json
from urllib.parse import urljoin


def send_request(url):
    # Выполнение запроса
    response = requests.get(url)
    response.raise_for_status()  # Проверка на успешность запроса
    return response


def parse_trade_name(soup: BeautifulSoup) -> str:
    trade_name_elem = soup.find('input', {'id': 'ctl00_plate_TradeNmR'})
    if trade_name_elem:
        trade_name = trade_name_elem['value']
    else:
        raise ValueError("Элемент trade_name не найден на странице.")

    return trade_name


def parse_owner_name(soup: BeautifulSoup) -> str:
    owner_name_elem = soup.find('div', {'id': 'ctl00_plate_MnfClNmR'})
    if owner_name_elem:
        owner_name = owner_name_elem.get_text(strip=False)
        # Извлечение текста в скобках, если он есть
        match = re.search(r'\((.*?)\)', owner_name)
        if match:
            owner_name = match.group(1)
    else:
        raise ValueError("Элемент owner_name не найден на странице.")

    return owner_name


def parse_owner_country(soup: BeautifulSoup) -> str:
    owner_country_elem = soup.find('textarea', {'id': 'ctl00_plate_CountryClR'})
    if owner_country_elem:
        owner_country = owner_country_elem.get_text(strip=False)
    else:
        raise ValueError("Элемент owner_country не найден на странице.")

    return owner_country


def parse_drug_forms(soup: BeautifulSoup) -> tuple[str, str, str, str]:
    drug_forms_table = soup.find('div', {'id': 'ctl00_plate_drugforms'})
    if drug_forms_table:
        inner_soup = BeautifulSoup(str(drug_forms_table), 'html.parser')
        dosage = soup.find_all('td', rowspan="2")[1].get_text(strip=True)
        expiry_date = inner_soup.find_all('td')[2].get_text(strip=True)
        storage_conditions = inner_soup.find_all('td')[3].get_text(strip=True)
        tablets_counts_ul = inner_soup.find('ul')
        try:
            tablets_counts = '+'.join([re.findall(r'\(([^)]+)\)', li.get_text(strip=True))[-1] for li in tablets_counts_ul.find_all('li')])
        except IndexError:
            tablets_counts = 'None'
    else:
        raise ValueError("Элемент drugforms не найден на странице.")

    return dosage, expiry_date, storage_conditions, tablets_counts


def parse_pharmacy_group(soup: BeautifulSoup) -> str:
    pharmacy_group_elem = soup.find('table', {'id': 'ctl00_plate_grFTG'})
    if pharmacy_group_elem:
        pharmacy_group = pharmacy_group_elem.find('td').text.strip().replace('; ', '+')
    else:
        raise ValueError("Элемент pharmaco_group не найден на странице.")

    return pharmacy_group


def save_pdf(soup: BeautifulSoup, url: str) -> str:
    pdf_filename = ""

    # Извлекаем торговое наименование лекарства
    trade_name_input = soup.find('input', {'id': 'ctl00_plate_TradeNmR'})
    trade_name = trade_name_input['value'] if trade_name_input else 'unknown'

    # Извлекаем ссылку на инструкцию в формате PDF
    instructions_button = soup.find('input', {'id': 'instructionsCaller'})
    if instructions_button:
        # Выполняем запрос для получения инструкций
        instructions_url = "https://grls.minzdrav.gov.ru/Grls_View_v2.aspx/AddInstrImg"
        reg_number = soup.find('input', {'id': 'ctl00_plate_RegNr'})['value']
        id_reg = soup.find('input', {'id': 'ctl00_plate_hfIdReg'})['value']

        payload = {
            "regNumber": reg_number,
            "idReg": id_reg
        }

        headers = {
            'Content-Type': 'application/json; charset=windows-1251'
        }

        response = requests.post(instructions_url, json=payload, headers=headers)
        response.raise_for_status()

        # Парсим JSON-ответ
        data = response.json()
        pdf_url = None

        if 'd' in data and data['d']:
            data = json.loads(data['d'])
            if 'Sources' in data and data['Sources']:
                for source in data['Sources']:
                    if 'Instructions' in source and source['Instructions']:
                        for instruction in source['Instructions']:
                            if 'Images' in instruction and instruction['Images']:
                                for image in instruction['Images']:
                                    if image['Url'].lower().endswith('.pdf'):
                                        pdf_url = urljoin(url, image['Url'])
                                        break

        if pdf_url:
            # Скачиваем PDF-файл
            pdf_response = requests.get(pdf_url)
            pdf_response.raise_for_status()

            # Сохраняем PDF-файл с именем, соответствующим торговому наименованию лекарства
            pdf_filename = f"data/instructions/{trade_name}.pdf"
            with open(pdf_filename, 'wb') as pdf_file:
                pdf_file.write(pdf_response.content)
        else:
            print("Ссылка на PDF-инструкцию не найдена")
    else:
        print("Кнопка для показа инструкций не найдена")

    return pdf_filename


def parse_data(soup: BeautifulSoup, url:str) -> dict[str, str]:
    trade_name = parse_trade_name(soup)
    owner_name = parse_owner_name(soup)
    owner_country = parse_owner_country(soup)
    owner_info = f"{owner_name}+{owner_country}".replace('"', '')
    dosage, expiry_date, storage_conditions, tablets_counts = parse_drug_forms(soup)
    pharmacy_group = parse_pharmacy_group(soup)
    prescription_required = 'По рецепту' in storage_conditions
    pdf_filename = save_pdf(soup, url)
    data = {
        "Торговое наименование": trade_name,
        "Наименование держателя": owner_info,
        "Дозировка": dosage,
        "Срок годности": expiry_date,
        "Условия хранения": storage_conditions,
        "Количество таблеток": tablets_counts,
        "Фармако-терапевтическая группа": pharmacy_group,
        "Рецептурное": prescription_required,
        "БАД": "false",
        "Файл с инструкцией": pdf_filename
    }

    return data


def urls_to_csv(urls: list[str], output_csv: str) -> None:
    # Определяем заголовки для CSV
    fieldnames = [
        "Торговое наименование",
        "Наименование держателя",
        "Дозировка",
        "Срок годности",
        "Условия хранения",
        "Количество таблеток",
        "Фармако-терапевтическая группа",
        "Рецептурное",
        "БАД",
        "Файл с инструкцией"
    ]

    # Создаем или перезаписываем CSV-файл
    with open(output_csv, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()  # Записываем заголовки

        for url in urls:
            # Загружаем HTML-страницу
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Парсим данные с помощью `parse_data`
            data = parse_data(soup, url)

            # Записываем данные в CSV
            writer.writerow(data)


if __name__ == "__main__":
    with open('data/urls.txt', 'r', encoding='utf-8') as file:
        urls = [line.strip() for line in file.readlines()]

    urls_to_csv(urls, "data/parsed_data.csv")
