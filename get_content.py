#!/usr/bin/env python
import os
import sys
import json
import configparser
import psycopg2
import requests
from datetime import datetime
from urllib.parse import urlparse
from loguru import logger
import time
import random
from tabulate import tabulate
from collections import defaultdict
import concurrent.futures

# Импорт библиотек для извлечения контента
import trafilatura

# Константы
NUM_PAGES_FROM_DOMAIN = 1000  # Максимальное количество страниц с одного домена
CONFIG_PATH = "config/config.ini"
COOKIES_DIR = "cookies"  # Директория с файлами cookie
LOGS_DIR = "logs"  # Директория для логов
MAX_RETRIES = 3  # Максимальное количество повторных попыток
MAX_WORKERS = 10  # Максимальное количество параллельных потоков
MAX_CONCURRENT_PER_DOMAIN = 2  # Максимальное количество параллельных запросов к одному домену
MAX_REDIRECTS = 5  # Максимальное количество редиректов
REQUEST_TIMEOUT = 40  # Таймаут запросов в секундах

# Статистика выполнения
stats = {
    'start_time': None,
    'end_time': None,
    'total_raw_pages': 0,
    'successful_raw_pages': 0,
    'failed_raw_pages': 0,
    'total_parsed_pages': 0,
    'total_content_size': 0,  # в байтах
    'domains_stats': defaultdict(lambda: {'success': 0, 'failed': 0, 'size': 0}),
    'parser_stats': {
        'trafilatura': {'success': 0, 'failed': 0, 'parsed': 0}
    }
}

# ---------------------------------------------------
# Настройка и конфигурация
# ---------------------------------------------------

def setup_logging():
    """Настройка логирования с использованием loguru"""
    # Проверяем существование директории для логов
    if not os.path.exists(LOGS_DIR):
        os.makedirs(LOGS_DIR)

    # Удаляем стандартный обработчик loguru
    logger.remove()

    # Добавляем обработчик для вывода в консоль с цветами
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        colorize=True
    )

    # Добавляем обработчик для записи в файл с ротацией (хранение последних 10 запусков)
    log_file_path = os.path.join(LOGS_DIR, "page_fetcher_{time}.log")
    logger.add(
        log_file_path,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO",
        rotation="1 day",
        retention=10
    )

    return logger

def load_config(config_path=CONFIG_PATH):
    """Загрузка конфигурации из файла"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    config = configparser.ConfigParser()
    config.read(config_path)

    if 'RSS-News.postgres_local' not in config:
        raise KeyError("Section 'RSS-News.postgres_local' not found in configuration file")

    return config['RSS-News.postgres_local']

# ---------------------------------------------------
# Функции для извлечения контента
# ---------------------------------------------------

def extract_content(html_content):
    """Извлечение контента с помощью библиотеки Trafilatura"""
    # Извлекаем основной контент
    extracted_text = trafilatura.extract(
        html_content,
        favor_precision=True,
        include_links=False,
        include_images=False,
        include_tables=True,
        include_comments=False,
        output_format='markdown',
        deduplicate=True
    )

    if not extracted_text:
        stats['parser_stats']['trafilatura']['failed'] += 1
        return None

    stats['parser_stats']['trafilatura']['success'] += 1
    return extracted_text

# ---------------------------------------------------
# Функции для работы с базой данных
# ---------------------------------------------------

def get_db_connection(db_config):
    """Создание подключения к базе данных"""
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password']
    )
    return conn

def get_pages_for_raw_content(conn):
    """
    Получение списка страниц для загрузки сырого контента
    Не более NUM_PAGES_FROM_DOMAIN страниц с одного домена,
    отсортированных по publication_date по убыванию
    """
    query = """
    WITH ranked_pages AS (
        SELECT
            np.id_page,
            np.page_url,
            s.domain,
            np.publication_date,
            ROW_NUMBER() OVER (PARTITION BY s.domain ORDER BY np.publication_date DESC) as rank
        FROM
            news_pages np
        JOIN
            sitemaps s ON np.id_sitemap = s.id_sitemap
        LEFT JOIN
            news_pages_content npc ON np.id_page = npc.id_page
        WHERE
            (npc.id_page IS NULL OR npc.page_raw_content IS NULL)
            AND np.is_error = FALSE
    )
    SELECT
        id_page,
        page_url,
        domain,
        publication_date
    FROM
        ranked_pages
    WHERE
        rank <= %s
    ORDER BY
        publication_date DESC;
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (NUM_PAGES_FROM_DOMAIN,))
            pages = cursor.fetchall()

        # Преобразуем результат в список словарей для удобства
        result = []
        for page in pages:
            result.append({
                'id_page': page[0],
                'page_url': page[1],
                'domain': page[2],
                'publication_date': page[3]
            })

        return result
    except Exception as e:
        logger.error(f"Error fetching pages list for raw content: {e}")
        raise

def get_pages_for_content_extraction(conn):
    """
    Получение списка страниц для извлечения контента
    Выбираются страницы, у которых есть сырой контент,
    но отсутствует контент
    """
    query = """
    SELECT
        npc.id_page,
        COALESCE(npc.page_url, np.page_url) as page_url,
        s.domain,
        COALESCE(npc.publication_date, np.publication_date) as publication_date
    FROM
        news_pages_content npc
    JOIN
        news_pages np ON npc.id_page = np.id_page
    JOIN
        sitemaps s ON np.id_sitemap = s.id_sitemap
    WHERE
        npc.page_raw_content IS NOT NULL
        AND npc.content IS NULL
        AND np.is_error = FALSE
    ORDER BY
        publication_date DESC;
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            pages = cursor.fetchall()

        # Преобразуем результат в список словарей для удобства
        result = []
        for page in pages:
            result.append({
                'id_page': page[0],
                'page_url': page[1],
                'domain': page[2],
                'publication_date': page[3]
            })

        return result
    except Exception as e:
        logger.error(f"Error fetching pages list for content extraction: {e}")
        raise

def save_raw_content_to_db(conn, content_info):
    """Сохранение сырого контента страницы в базу данных"""
    insert_query = """
    INSERT INTO news_pages_content
        (id_page, page_url, publication_date, page_raw_content)
    VALUES
        (%s, %s, %s, %s)
    ON CONFLICT (id_page) DO UPDATE SET
        page_url = EXCLUDED.page_url,
        publication_date = EXCLUDED.publication_date,
        page_raw_content = EXCLUDED.page_raw_content,
        fetched_at = NOW()
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                insert_query,
                (
                    content_info['id_page'],
                    content_info.get('page_url'),
                    content_info.get('publication_date'),
                    content_info['page_raw_content']
                )
            )
        conn.commit()
        status = "successfully" if content_info['page_raw_content'] else "with error"
        logger.info(f"Raw content for page ID={content_info['id_page']} saved to DB {status}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving raw content to DB: {e}")
        return False

def get_raw_content_from_db(conn, id_page):
    """Получение сырого контента из базы данных"""
    query = """
    SELECT
        page_raw_content,
        content
    FROM
        news_pages_content
    WHERE
        id_page = %s
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (id_page,))
            result = cursor.fetchone()

        if not result:
            logger.error(f"No raw content found for page ID={id_page}")
            return None, None

        return result[0], result[1]
    except Exception as e:
        logger.error(f"Error fetching raw content from DB: {e}")
        return None, None

def save_extracted_content_to_db(conn, content_info):
    """Сохранение извлеченного контента в базу данных"""
    update_query = """
    UPDATE news_pages_content
    SET
        content = %s
    WHERE
        id_page = %s
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                update_query,
                (
                    content_info.get('content'),
                    content_info['id_page']
                )
            )
        conn.commit()
        logger.info(f"Extracted content for page ID={content_info['id_page']} saved to DB")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving extracted content to DB: {e}")
        return False

def update_page_error_status(conn, id_page, is_error):
    """Обновление статуса ошибки страницы в базе данных"""
    update_query = """
    UPDATE news_pages
    SET
        is_error = %s
    WHERE
        id_page = %s
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                update_query,
                (
                    is_error,
                    id_page
                )
            )
        conn.commit()
        logger.info(f"Error status for page ID={id_page} updated to {is_error}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating error status in DB: {e}")
        return False

# ---------------------------------------------------
# Функции для работы с HTTP и cookies
# ---------------------------------------------------

def extract_domain_from_url(url):
    """Извлечение домена из URL"""
    parsed_url = urlparse(url)
    domain = parsed_url.netloc

    # Убираем префикс www. если он есть
    if domain.startswith('www.'):
        domain = domain[4:]

    return domain

def load_cookies(domain):
    """Загрузка cookies для указанного домена, используя самый свежий файл"""
    cookie_files = []

    # Проверяем существование директории
    if not os.path.exists(COOKIES_DIR):
        logger.warning(f"Cookies directory not found: {COOKIES_DIR}")
        return []

    # Ищем все файлы для указанного домена
    for filename in os.listdir(COOKIES_DIR):
        if filename.startswith(f"{domain}_") and filename.endswith('.json'):
            cookie_files.append(filename)

    if not cookie_files:
        logger.warning(f"No cookie files found for domain: {domain}")
        return []

    # Парсим даты из имен файлов
    date_files = []
    for filename in cookie_files:
        # Извлекаем дату из имени файла (формат domain_DD-MM-YYYY.json)
        date_str = filename.replace(f"{domain}_", "").replace(".json", "")
        try:
            file_date = datetime.strptime(date_str, "%d-%m-%Y")
            date_files.append((file_date, filename))
        except ValueError:
            logger.warning(f"Invalid date format in cookie file: {filename}")
            continue

    if not date_files:
        logger.warning(f"No valid cookie files with dates found for domain: {domain}")
        return []

    # Находим самый свежий файл
    latest_file = max(date_files, key=lambda x: x[0])[1]
    cookie_path = os.path.join(COOKIES_DIR, latest_file)

    logger.info(f"Using cookie file: {latest_file}")

    try:
        with open(cookie_path, 'r') as f:
            cookies = json.load(f)
        return cookies
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Error loading cookie file {latest_file}: {e}")
        return []

def cookies_to_dict(cookies_list):
    """Преобразование списка cookies в словарь для requests"""
    cookies_dict = {}
    for cookie in cookies_list:
        cookies_dict[cookie['name']] = cookie['value']
    return cookies_dict

def get_default_headers():
    """Получение стандартных HTTP заголовков"""
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }

def fetch_page_content(page_info, retry=0, redirect_count=0):
    """Скачивание HTML-контента страницы с поддержкой повторных попыток и обработкой редиректов"""
    url = page_info['page_url']
    domain = page_info.get('domain')

    # Если домен не указан, извлекаем его из URL
    if not domain:
        domain = extract_domain_from_url(url)

    # Загрузка cookies
    cookies_list = load_cookies(domain)
    cookies_dict = cookies_to_dict(cookies_list)

    # Получение заголовков
    headers = get_default_headers()

    try:
        # Прямой запрос без автоматического следования редиректам
        response = requests.get(
            url,
            cookies=cookies_dict,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=False
        )

        # Обработка редиректов вручную
        if response.is_redirect and redirect_count < MAX_REDIRECTS:
            redirect_url = response.headers.get('Location')

            # Если ссылка относительная, преобразуем в абсолютную
            if redirect_url and not redirect_url.startswith(('http://', 'https://')):
                # Собираем полный URL для относительного редиректа
                parsed_url = urlparse(url)
                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                redirect_url = f"{base_url}{redirect_url if redirect_url.startswith('/') else '/' + redirect_url}"

            if redirect_url:
                logger.info(f"Редирект {redirect_count+1}/{MAX_REDIRECTS}: {url} -> {redirect_url}")

                # Создаем новый page_info с обновленным URL
                new_page_info = page_info.copy()
                new_page_info['page_url'] = redirect_url

                # Увеличиваем счетчик редиректов и рекурсивно делаем запрос
                return fetch_page_content(new_page_info, retry, redirect_count + 1)

        # Если был 3xx/4xx/5xx статус, но не редирект
        response.raise_for_status()

        html_content = response.text

        # Создаем базовый результат
        result = {
            'id_page': page_info['id_page'],
            'page_url': page_info['page_url'],
            'publication_date': page_info.get('publication_date'),
            'page_raw_content': html_content,
            'content_size': len(html_content)
        }

        return result
    except requests.exceptions.RequestException as e:
        # Если не превышено максимальное количество попыток, пробуем снова
        if retry < MAX_RETRIES - 1:
            logger.warning(f"Error loading {url}, attempt {retry + 1}/{MAX_RETRIES}: {e}")
            # Экспоненциальная задержка с небольшой случайностью
            delay = (2 ** retry) + random.uniform(0, 1)
            time.sleep(delay)
            return fetch_page_content(page_info, retry + 1, redirect_count)

        logger.error(f"Failed to load page {url} after {MAX_RETRIES} attempts: {e}")

        # Устанавливаем флаг ошибки для этой страницы в базе данных
        config = load_config()
        conn = get_db_connection(config)
        update_page_error_status(conn, page_info['id_page'], True)
        conn.close()

        return {
            'id_page': page_info['id_page'],
            'page_url': page_info['page_url'],
            'publication_date': page_info.get('publication_date'),
            'page_raw_content': None,
            'content_size': 0,
            'is_error': True
        }

# ---------------------------------------------------
# Функции для обработки страниц и многопоточности
# ---------------------------------------------------

def process_page_raw_content(page, db_config):
    """Загрузка и сохранение сырого контента страницы"""
    # Создаем отдельное подключение для каждого потока
    conn = get_db_connection(db_config)

    # Скачивание контента
    content_info = fetch_page_content(page)

    # Сохранение контента в БД
    if content_info and 'id_page' in content_info:
        success = save_raw_content_to_db(conn, content_info)
        domain = page['domain']

        # Обновляем статистику
        if success and content_info['page_raw_content']:
            stats['successful_raw_pages'] += 1
            stats['domains_stats'][domain]['success'] += 1
            content_size = content_info['content_size']
            stats['total_content_size'] += content_size
            stats['domains_stats'][domain]['size'] += content_size
        else:
            stats['failed_raw_pages'] += 1
            stats['domains_stats'][domain]['failed'] += 1

            # Если установлен флаг ошибки, обновим статус в БД
            if content_info.get('is_error', False):
                update_page_error_status(conn, content_info['id_page'], True)
    else:
        success = False
        stats['failed_raw_pages'] += 1
        domain = page['domain']
        stats['domains_stats'][domain]['failed'] += 1

    # Закрытие соединения
    conn.close()

    # Случайная задержка между запросами для снижения нагрузки на сервер
    time.sleep(random.uniform(0.5, 2.0))

    return success

def process_page_content_extraction(page, db_config):
    """Извлечение контента из сырого HTML и сохранение в БД"""
    # Создаем отдельное подключение для каждого потока
    conn = get_db_connection(db_config)

    # Получаем сырой контент и текущие значения
    raw_content, current_content = get_raw_content_from_db(conn, page['id_page'])

    if not raw_content:
        logger.error(f"No raw content found for page ID={page['id_page']}")
        conn.close()
        return False

    # Готовим словарь с извлеченным контентом
    extracted_content = {}

    # Проверяем и извлекаем контент, если нужно
    if current_content is None:
        content = extract_content(raw_content)
        extracted_content['content'] = content
        stats['parser_stats']['trafilatura']['parsed'] += 1 if content else 0

    # Если есть что сохранять, сохраняем
    if extracted_content:
        extracted_content['id_page'] = page['id_page']
        success = save_extracted_content_to_db(conn, extracted_content)
    else:
        logger.info(f"No content to extract for page ID={page['id_page']}")
        success = True

    # Закрытие соединения
    conn.close()

    return success

def process_pages(pages, process_func, db_config):
    """Параллельная обработка страниц с использованием ThreadPoolExecutor"""
    results = []

    # Распределение страниц по доменам для ограничения параллельных запросов к одному домену
    domain_pages = defaultdict(list)
    for page in pages:
        domain_pages[page['domain']].append(page)

    # Обрабатываем каждый домен отдельно с ограничением параллельных запросов
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Держим учет всех запущенных задач
        all_futures = []

        # Для каждого домена запускаем обработку его страниц
        for domain, domain_page_list in domain_pages.items():
            # Разбиваем страницы домена на батчи для ограничения параллельности
            batches = [domain_page_list[i:i + MAX_CONCURRENT_PER_DOMAIN]
                     for i in range(0, len(domain_page_list), MAX_CONCURRENT_PER_DOMAIN)]

            # Обрабатываем батчи последовательно
            for batch in batches:
                batch_futures = []
                for page in batch:
                    future = executor.submit(process_func, page, db_config)
                    batch_futures.append(future)
                    all_futures.append((future, page))

                # Ждем завершения текущего батча перед запуском следующего
                for future in concurrent.futures.as_completed(batch_futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Exception in batch processing: {e}")

                # Небольшая задержка между батчами одного домена
                time.sleep(random.uniform(0.2, 0.5))

        # Собираем результаты всех задач
        for future, page in all_futures:
            try:
                result = future.result()
                results.append(result)
                logger.info(f"Completed processing page: {page['page_url']}")
            except Exception as e:
                logger.error(f"Exception processing page {page['page_url']}: {e}")
                results.append(False)

    return results

# ---------------------------------------------------
# Функции для отображения статистики
# ---------------------------------------------------

def format_size(size_bytes):
    """Форматирование размера в удобочитаемый вид"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

def format_time(seconds):
    """Форматирование времени в удобочитаемый вид"""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds // 60
        sec = seconds % 60
        return f"{int(minutes)} min {int(sec)} sec"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        sec = seconds % 60
        return f"{int(hours)} hours {int(minutes)} min {int(sec)} sec"

def display_statistics():
    """Отображение статистики работы скрипта"""
    if not stats['start_time'] or not stats['end_time']:
        return

    execution_time = (stats['end_time'] - stats['start_time']).total_seconds()

    # Защита от деления на ноль
    success_rate_raw = "0%"
    if stats['total_raw_pages'] > 0:
        success_rate_raw = f"{(stats['successful_raw_pages'] / stats['total_raw_pages'] * 100):.2f}%"

    avg_page_size = "0 B"
    if stats['successful_raw_pages'] > 0:
        avg_page_size = format_size(stats['total_content_size'] / stats['successful_raw_pages'])

    pages_per_second = "0"
    if execution_time > 0:
        pages_per_second = f"{(stats['total_raw_pages'] + stats['total_parsed_pages']) / execution_time:.2f}"

    # Общая статистика
    general_stats = [
        ["Execution time", format_time(execution_time)],
        ["Total raw pages processed", stats['total_raw_pages']],
        ["Successful raw pages", stats['successful_raw_pages']],
        ["Failed raw pages", stats['failed_raw_pages']],
        ["Total pages parsed", stats['total_parsed_pages']],
        ["Success rate (raw)", success_rate_raw],
        ["Total content size", format_size(stats['total_content_size'])],
        ["Average page size", avg_page_size],
        ["Pages per second", pages_per_second]
    ]

    # Статистика по доменам
    domains_stats = []
    for domain, domain_stats in sorted(stats['domains_stats'].items()):
        total = domain_stats['success'] + domain_stats['failed']
        success_rate = 0
        if total > 0:
            success_rate = (domain_stats['success'] / total * 100)
        domains_stats.append([
            domain,
            domain_stats['success'],
            domain_stats['failed'],
            f"{success_rate:.2f}%",
            format_size(domain_stats['size'])
        ])

    # Статистика по парсерам
    parser_stats = []
    for parser_name, parser_data in stats['parser_stats'].items():
        total = parser_data['success'] + parser_data['failed']
        success_rate = "0%"
        if total > 0:
            success_rate = f"{(parser_data['success'] / total * 100):.2f}%"
        parser_stats.append([
            parser_name.capitalize(),
            parser_data['success'],
            parser_data['failed'],
            parser_data['parsed'],
            success_rate
        ])

    # Выводим общую статистику
    print("\n✅ GENERAL STATISTICS:")
    print(tabulate(general_stats, tablefmt="pretty"))

    print("\n🌐 DOMAIN STATISTICS:")
    print(tabulate(domains_stats, headers=["Domain", "Success", "Failed", "Success Rate", "Content Size"], tablefmt="pretty"))

    print("\n📑 PARSER STATISTICS:")
    print(tabulate(parser_stats, headers=["Parser", "Success", "Failed", "Parsed", "Success Rate"], tablefmt="pretty"))

# ---------------------------------------------------
# Основная логика
# ---------------------------------------------------

def main():
    """Основная функция"""
    # Инициализация статистики
    stats['start_time'] = datetime.now()

    # Настройка логирования
    setup_logging()

    # Загрузка конфигурации
    db_config = load_config()

    # Подключение к БД
    conn = get_db_connection(db_config)

    # Этап 1: Получение сырого контента
    logger.info("Stage 1: Fetching raw content")
    pages_for_raw = get_pages_for_raw_content(conn)
    stats['total_raw_pages'] = len(pages_for_raw)
    logger.info(f"Found {len(pages_for_raw)} pages for raw content fetching")

    if pages_for_raw:
        # Последовательная обработка для получения сырого контента
        results_raw = process_pages(pages_for_raw, process_page_raw_content, db_config)
        success_count_raw = results_raw.count(True)
        logger.info(f"Processed {len(pages_for_raw)} raw pages, successful: {success_count_raw}, with errors: {len(pages_for_raw) - success_count_raw}")
    else:
        logger.info("No pages for raw content fetching")

    # Этап 2: Извлечение контента
    logger.info("Stage 2: Extracting content")
    pages_for_extraction = get_pages_for_content_extraction(conn)
    stats['total_parsed_pages'] = len(pages_for_extraction)
    logger.info(f"Found {len(pages_for_extraction)} pages for content extraction")

    if pages_for_extraction:
        # Последовательная обработка для извлечения контента
        results_extraction = process_pages(pages_for_extraction, process_page_content_extraction, db_config)
        success_count_extraction = results_extraction.count(True)
        logger.info(f"Processed {len(pages_for_extraction)} pages for extraction, successful: {success_count_extraction}, with errors: {len(pages_for_extraction) - success_count_extraction}")
    else:
        logger.info("No pages for content extraction")

    # Закрываем соединение
    conn.close()

    # Фиксируем время окончания
    stats['end_time'] = datetime.now()

    # Отображаем статистику
    display_statistics()

    logger.info("Script executed successfully")

if __name__ == "__main__":
    main()