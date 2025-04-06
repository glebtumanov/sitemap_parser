#!/usr/bin/env python
import json
import time
import os
import tempfile
from datetime import datetime
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Глобальные переменные
SITEMAP_FILE_PATH = "sitemaps.txt"  # Файл с URL сайтемапов
COOKIE_PATH = "cookies"  # Папка для сохранения файлов с куками

# Создаем папку для куков, если ее нет
if not os.path.exists(COOKIE_PATH):
    os.makedirs(COOKIE_PATH)

# Функция для проверки существования cookie за сегодняшний день
def cookie_exists_for_today(domain):
    current_date = datetime.now().strftime("%d-%m-%Y")
    filename = f"{domain}_{current_date}.json"
    filepath = os.path.join(COOKIE_PATH, filename)
    return os.path.exists(filepath)

# Функция для извлечения доменов из файла сайтемапов
def extract_domains_from_sitemap_file(file_path):
    domains = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                url = line.strip()
                if url:
                    parsed_url = urlparse(url)
                    domain = parsed_url.netloc
                    # Добавляем http/https для полного URL
                    full_url = f"{parsed_url.scheme}://{domain}" if parsed_url.scheme else f"https://{domain}"
                    if domain and full_url not in domains:
                        domains.append(full_url)
        print(f"Загружено {len(domains)} доменов из файла {file_path}")
        return domains
    except Exception as e:
        print(f"Ошибка при чтении файла {file_path}: {e}")
        return []

# Получаем список доменов из файла сайтемапов
URLS = extract_domains_from_sitemap_file(SITEMAP_FILE_PATH)

if not URLS:
    print("Не удалось загрузить домены из файла. Завершение работы.")
    exit(1)

# Создаем временную папку для профиля Chrome
temp_dir = tempfile.mkdtemp()

# Настройки браузера
options = Options()
options.add_argument("--headless=new")  # Новый headless-режим
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--disable-software-rasterizer")
options.add_argument("--remote-debugging-port=9222")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument(f"--user-data-dir={temp_dir}")  # Используем уникальный временный профиль
options.add_argument("--window-size=1920x1080")
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

# Запуск драйвера Chrome
service = Service(ChromeDriverManager(driver_version="134.0.6998.35").install())
driver = webdriver.Chrome(service=service, options=options)

# Обрабатываем каждый URL из списка
for url in URLS:
    # Извлекаем доменное имя из URL
    parsed_url = urlparse(url)
    domain = parsed_url.netloc

    # Проверяем, существуют ли уже cookie за сегодня
    if cookie_exists_for_today(domain):
        log_entry = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Cookie для {domain} уже существуют. Пропускаем."
        print(log_entry)
        continue

    driver.get(url)
    time.sleep(10)  # даем время на выполнение JS

    # Получаем куки
    cookies = driver.get_cookies()

    # Формируем имя файла: "<домен>_<дата>.json"
    current_date = datetime.now().strftime("%d-%m-%Y")
    filename = f"{domain}_{current_date}.json"
    filepath = os.path.join(COOKIE_PATH, filename)

    # Сохраняем куки в JSON-файл
    with open(filepath, "w") as f:
        json.dump(cookies, f, indent=4)

    # Удаляем старые файлы с куками для данного домена (кроме только что созданного)
    for file in os.listdir(COOKIE_PATH):
        if file.startswith(domain + "_") and file != filename:
            os.remove(os.path.join(COOKIE_PATH, file))

    # Записываем краткий лог
    log_entry = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Cookies сохранены для {domain} [{len(cookies)}]"
    print(log_entry)

# Завершаем работу драйвера
driver.quit()

print("Скрипт выполнен успешно!")
