from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re

# Инициализация драйвера
driver = webdriver.Chrome()

token = input("Введите токен из ссылки: ")

# URL страницы для выгрузки
url = f"https://grls.minzdrav.gov.ru/GRLS.aspx?RegNumber=&MnnR=&lf=%d0%a2%d0%b0%d0%b1%d0%bb%d0%b5%d1%82%d0%ba%d0%b8&TradeNmR=&OwnerName=&MnfOrg=&MnfOrgCountry=&isfs=0&regtype=1%2c6&pageSize=99&token={token}&order=Registered&orderType=desc&pageNum="
for i in range(1,63):
    # Открываем страницу
    driver.get(url+str(i))

    # Ждем, пока таблица с лекарствами загрузится
    wait = WebDriverWait(driver, 20)
    table = wait.until(EC.presence_of_element_located((By.ID, 'ctl00_plate_gr')))
    # Извлекаем все строки таблицы
    rows = table.find_elements(By.CLASS_NAME, 'poi')

    # Открываем файл для записи (режим 'a' - добавление)
    with open('data/urls.txt', 'a', encoding='utf-8') as file:
        for row in rows:
            # Ищем текст между одинарными кавычками в атрибуте onclick
            match = re.search(r"'(.*?)'", row.get_attribute('onclick'))
            if match:
                # Записываем найденный текст в файл, добавляя перенос строки
                file.write(f'https://grls.minzdrav.gov.ru/Grls_View_v2.aspx?routingGuid={match.group(1)}\n')

    driver.quit()
