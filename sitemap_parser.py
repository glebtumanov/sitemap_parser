#!/usr/bin/env python
import requests
import xml.etree.ElementTree as ET
import re
import psycopg2
import datetime
import dateparser
import configparser
import json
import os
from loguru import logger
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from tabulate import tabulate

# Настройка логирования с помощью loguru
# Удаляем стандартный обработчик
logger.remove()
# Добавляем обработчик для вывода в консоль с цветовой подсветкой
logger.add(
    sink=lambda msg: print(msg, end=""),
    colorize=True,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)
# Добавляем обработчик для записи в файл с ротацией (хранить только 10 последних запусков)
os.makedirs("logs", exist_ok=True)
logger.add(
    sink="logs/sitemap_parser_{time}.log",
    rotation="1 day",
    retention=10,
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
)

# Путь к файлу со списком мастер-sitemap
SITEMAP_FILE_PATH = "sitemaps.txt"
# Количество месяцев для проверки "свежести" новостей
CHECK_LAST_MONTHS = 12

# Константы
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/116.0'
}
MAX_RETRIES = 3
RETRY_DELAY = 1
MAX_WORKERS = 5
MAX_REDIRECTS = 5  # Максимальное количество редиректов
TIMEOUT = 60

# Функция для вычисления минимальной даты на основе текущей даты и количества месяцев
def get_min_date() -> datetime.datetime:
    """Вычисляет минимальную дату на основе текущей даты и CHECK_LAST_MONTHS."""
    current_date = datetime.datetime.now(datetime.timezone.utc)
    min_date = current_date - relativedelta(months=CHECK_LAST_MONTHS)
    return min_date

# Функция для загрузки конфигурации БД
def load_db_config():
    """Загрузить конфигурацию для подключения к PostgreSQL из файла."""
    config = configparser.ConfigParser()
    config.read("config/config.ini")

    if 'RSS-News.postgres_local' not in config:
        raise ValueError("Section 'RSS-News.postgres_local' not found in config file")

    return {
        'host': config['RSS-News.postgres_local']['host'],
        'port': config['RSS-News.postgres_local']['port'],
        'database': config['RSS-News.postgres_local']['dbname'],
        'user': config['RSS-News.postgres_local']['user'],
        'password': config['RSS-News.postgres_local']['password']
    }

# Класс для хранения статистики по сайту
@dataclass
class SiteStats:
    sitemaps_processed: int = 0
    pages_added: int = 0
    pages_skipped: int = 0
    pages_no_date: int = 0  # Новое поле для страниц без даты
    min_date: Optional[datetime.datetime] = None
    max_date: Optional[datetime.datetime] = None

    def update_date_range(self, date: Optional[datetime.datetime]):
        """Обновляет диапазон дат."""
        if date is None:
            return

        if self.min_date is None or date < self.min_date:
            self.min_date = date

        if self.max_date is None or date > self.max_date:
            self.max_date = date

# Датаклассы для хранения информации
@dataclass
class Sitemap:
    url: str
    is_master: bool
    parent_id: Optional[int] = None
    last_mod: Optional[datetime.datetime] = None
    id_sitemap: Optional[int] = None
    is_fresh: bool = False

@dataclass
class NewsPage:
    page_url: str
    id_sitemap: int
    publication_date: Optional[datetime.datetime] = None
    is_error: bool = False

def get_domain(url: str) -> str:
    """Извлечь домен из URL."""
    parsed_url = urlparse(url)
    return parsed_url.netloc

def load_cookies(domain: str) -> Dict[str, str]:
    """Загружает cookies для указанного домена из соответствующего файла."""
    today = datetime.datetime.now().strftime("%d-%m-%Y")
    cookies_file = f"{domain}_{today}.json"

    try:
        if os.path.exists(cookies_file):
            with open(cookies_file, 'r') as f:
                cookies_list = json.load(f)

                # Преобразуем список словарей cookies в словарь для requests
                cookies_dict = {}
                for cookie in cookies_list:
                    cookies_dict[cookie['name']] = cookie['value']

                logger.info(f"Loaded {len(cookies_dict)} cookies for {domain}")
                return cookies_dict
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Could not load cookies for {domain}: {e}")

    return {}

def fetch_xml(url: str) -> Optional[ET.Element]:
    """Загрузить XML по URL с повторными попытками, cookies и обработкой редиректов."""
    domain = get_domain(url)
    cookies_dict = load_cookies(domain)
    redirect_count = 0
    current_url = url

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                current_url,
                headers=HEADERS,
                cookies=cookies_dict,
                timeout=TIMEOUT,
                allow_redirects=False
            )

            # Обработка редиректов вручную
            while response.is_redirect and redirect_count < MAX_REDIRECTS:
                redirect_count += 1
                redirect_url = response.headers.get('Location')

                # Обработка относительных URL
                if redirect_url.startswith('/'):
                    parsed_url = urlparse(current_url)
                    redirect_url = f"{parsed_url.scheme}://{parsed_url.netloc}{redirect_url}"

                logger.debug(f"Redirecting to {redirect_url} (redirect {redirect_count}/{MAX_REDIRECTS})")
                current_url = redirect_url

                # Проверяем домен редиректа, если отличается - загружаем новые cookies
                redirect_domain = get_domain(current_url)
                if redirect_domain != domain:
                    domain = redirect_domain
                    cookies_dict = load_cookies(domain)
                    logger.debug(f"Domain changed to {domain}, loaded new cookies")

                response = requests.get(
                    current_url,
                    headers=HEADERS,
                    cookies=cookies_dict,
                    timeout=TIMEOUT,
                    allow_redirects=False
                )

            if redirect_count >= MAX_REDIRECTS:
                logger.warning(f"Too many redirects for {url}, max {MAX_REDIRECTS} allowed")
                return None

            response.raise_for_status()

            # Более безопасное удаление XML-декларации и стилей
            content = response.text
            try:
                # Пытаемся сначала распарсить как есть
                root = ET.fromstring(content)
                return root
            except ET.ParseError:
                # Если не получилось, пробуем удалить XML-декларацию и стили
                content = re.sub(r'<\?xml[^>]+\?>', '', content)
                content = re.sub(r'<\?xml-stylesheet[^>]+\?>', '', content)
                try:
                    root = ET.fromstring(content)
                    return root
                except ET.ParseError as e:
                    logger.error(f"XML parsing error after removing declarations for {url}: {e}")
                    return None
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt+1}/{MAX_RETRIES} failed for {url}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        except ET.ParseError as e:
            logger.error(f"XML parsing error for {url}: {e}")
            return None

    logger.error(f"Failed to load XML after {MAX_RETRIES} attempts for {url}")
    return None

def parse_date(date_str: Optional[str]) -> Optional[datetime.datetime]:
    """Парсинг даты и конвертация в UTC используя dateparser."""
    if not date_str:
        return None

    try:
        # Удаляем CDATA если есть
        if '<![CDATA[' in date_str and ']]>' in date_str:
            date_str = date_str.replace('<![CDATA[', '').replace(']]>', '')

        # Удаляем HTML-теги, если они есть
        date_str = re.sub(r'<[^>]+>', '', date_str)

        # Очищаем строку от лишних пробелов
        date_str = date_str.strip()

        # Пропускаем явно некорректные значения
        if date_str in ['0000-00-00', '1970-01-01', '0001-01-01']:
            logger.warning(f"Invalid date value found: {date_str}")
            return None

        # Используем dateparser для парсинга даты с расширенными настройками
        dt = dateparser.parse(
            date_str,
            settings={
                'TIMEZONE': 'UTC',        # Установка временной зоны по умолчанию
                'RETURN_AS_TIMEZONE_AWARE': True,  # Возврат даты с учетом временной зоны
                'STRICT_PARSING': False   # Нестрогий парсинг для поддержки различных форматов
            }
        )

        # Если dateparser не смог распознать дату
        if dt is None:
            # Пробуем разные форматы для сложных случаев
            for fmt in [
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%dT%H:%M:%S.%f%z',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%d.%m.%Y %H:%M:%S',
                '%d.%m.%Y',
                '%Y/%m/%d',
                '%d/%m/%Y'
            ]:
                try:
                    dt = datetime.datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue

        # Если все еще нет результата, возвращаем None
        if dt is None:
            logger.warning(f"Could not parse date string: {date_str}")
            return None

        # Убедимся, что у нас есть информация о временной зоне
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            # Конвертируем в UTC
            dt = dt.astimezone(datetime.timezone.utc)

        return dt
    except Exception as e:
        logger.error(f"Error parsing date {date_str}: {e}")
        return None

def print_stats_box(domain: str, stats: SiteStats):
    """Выводит красивую рамку со статистикой по сайту."""
    # Используем loguru для вывода статистики с форматированием
    logger.info("=" * 80)
    logger.info(f"СТАТИСТИКА ДЛЯ ДОМЕНА: {domain}")
    logger.info("-" * 80)
    logger.info(f"Обработано sitemap: {stats.sitemaps_processed}")
    logger.info(f"Добавлено страниц: {stats.pages_added}")
    logger.info(f"Пропущено страниц (устаревшие): {stats.pages_skipped}")
    logger.info(f"Пропущено страниц (без даты): {stats.pages_no_date}")

    # Формируем строку с датами
    if stats.min_date and stats.max_date:
        min_date_str = stats.min_date.strftime("%Y-%m-%d %H:%M:%S UTC")
        max_date_str = stats.max_date.strftime("%Y-%m-%d %H:%M:%S UTC")
        logger.info(f"Даты публикаций: с {min_date_str} по {max_date_str}")
    else:
        logger.info("Даты публикаций: не найдены")

    logger.info("=" * 80)

class SitemapProcessor:
    def __init__(self):
        self.db_config = load_db_config()
        self.namespaces = {
            'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            'news': 'http://www.google.com/schemas/sitemap-news/0.9',
            'image': 'http://www.google.com/schemas/sitemap-image/1.1',
            'xhtml': 'http://www.w3.org/1999/xhtml',
            'mobile': 'http://www.google.com/schemas/sitemap-mobile/1.0',
            'video': 'http://www.google.com/schemas/sitemap-video/1.1'
        }
        # Словарь для хранения статистики по доменам
        self.site_stats = defaultdict(SiteStats)
        # Минимальная дата для поиска новостей
        self.min_date = get_min_date()
        logger.info(f"Минимальная дата для поиска новостей: {self.min_date.strftime('%Y-%m-%d')}")

    def get_connection(self):
        """Создать подключение к базе данных."""
        try:
            return psycopg2.connect(**self.db_config)
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            raise

    def save_sitemap(self, sitemap: Sitemap) -> int:
        """Сохранить информацию о sitemap в БД."""
        domain = get_domain(sitemap.url)

        # Мастер sitemap всегда должен быть помечен как свежий
        if sitemap.is_master:
            sitemap.is_fresh = True

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # Проверяем, существует ли sitemap с таким URL
                    cursor.execute(
                        "SELECT id_sitemap FROM sitemaps WHERE sitemap_url = %s",
                        (sitemap.url,)
                    )
                    result = cursor.fetchone()

                    if result:
                        # Если sitemap существует, возвращаем его ID
                        # Если это мастер-sitemap, обновляем его флаг is_fresh на True
                        if sitemap.is_master:
                            cursor.execute("""
                                UPDATE sitemaps
                                SET is_fresh = TRUE
                                WHERE id_sitemap = %s
                            """, (result[0],))
                            conn.commit()
                        return result[0]

                    # Вставляем новый sitemap
                    cursor.execute("""
                        INSERT INTO sitemaps (domain, sitemap_url, is_master, parent_id, last_mod, is_fresh)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id_sitemap
                    """, (
                        domain,
                        sitemap.url,
                        sitemap.is_master,
                        sitemap.parent_id,
                        sitemap.last_mod,
                        sitemap.is_fresh
                    ))

                    sitemap_id = cursor.fetchone()[0]
                    conn.commit()

                    # Увеличиваем счетчик обработанных sitemap
                    self.site_stats[domain].sitemaps_processed += 1

                    return sitemap_id

                except psycopg2.Error as e:
                    conn.rollback()
                    logger.error(f"Error saving sitemap {sitemap.url}: {e}")
                    raise

    def update_sitemap_freshness(self, sitemap_id: int, is_fresh: bool):
        """Обновить флаг свежести sitemap в БД."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        UPDATE sitemaps
                        SET is_fresh = %s, processed_at = NOW()
                        WHERE id_sitemap = %s
                    """, (is_fresh, sitemap_id))
                    conn.commit()
                except psycopg2.Error as e:
                    conn.rollback()
                    logger.error(f"Error updating sitemap freshness for ID {sitemap_id}: {e}")

    def save_news_page(self, news_page: NewsPage, domain: str) -> Optional[int]:
        """Сохранить информацию о странице новостей в БД."""
        # Проверяем, является ли домен новым
        is_new_domain = self.is_new_domain(domain)

        # Проверяем дату публикации по минимальной допустимой дате
        # Для новых доменов пропускаем эту проверку
        if not is_new_domain and (news_page.publication_date is None or news_page.publication_date < self.min_date):
            months_ago = CHECK_LAST_MONTHS
            logger.debug(f"Skipping page {news_page.page_url} - publication date is older than {months_ago} months")
            self.site_stats[domain].pages_skipped += 1
            return None

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # Проверяем, существует ли страница с таким URL
                    cursor.execute(
                        "SELECT id_page FROM news_pages WHERE page_url = %s",
                        (news_page.page_url,)
                    )
                    result = cursor.fetchone()

                    if result:
                        # Если страница существует, увеличиваем счетчик пропущенных
                        self.site_stats[domain].pages_skipped += 1
                        return result[0]

                    # Вставляем новую страницу
                    cursor.execute("""
                        INSERT INTO news_pages (id_sitemap, page_url, publication_date, is_error)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id_page
                    """, (
                        news_page.id_sitemap,
                        news_page.page_url,
                        news_page.publication_date,
                        news_page.is_error
                    ))

                    page_id = cursor.fetchone()[0]
                    conn.commit()

                    # Увеличиваем счетчик добавленных страниц
                    self.site_stats[domain].pages_added += 1

                    # Обновляем информацию о диапазоне дат
                    self.site_stats[domain].update_date_range(news_page.publication_date)

                    return page_id

                except psycopg2.Error as e:
                    conn.rollback()
                    logger.error(f"Error saving page {news_page.page_url}: {e}")
                    return None

    def check_sitemap_freshness(self, root) -> bool:
        """
        Проверяет, содержит ли sitemap новости с датами публикации не старше CHECK_LAST_MONTHS месяцев.
        Sitemap считается "свежим", если хотя бы одна страница имеет дату не старше минимальной.
        """
        # Определяем, какое пространство имен используется в XML
        namespace = root.tag.split('}')[0] + '}' if '}' in root.tag else ''

        # Проверяем разные варианты поиска URL элементов
        url_elements = []
        search_paths = [
            # С пространством имен по умолчанию
            f".//{namespace}url",
            # С префиксом sm
            ".//sm:url",
            # Без пространства имен
            ".//url"
        ]

        for path in search_paths:
            try:
                if path.startswith(".//sm:"):
                    elements = root.findall(path, self.namespaces)
                else:
                    elements = root.findall(path)

                if elements:
                    url_elements = elements
                    logger.debug(f"Found URL elements using path: {path}")
                    break
            except Exception as e:
                logger.warning(f"Error searching with path {path}: {e}")

        logger.debug(f"Checking freshness of sitemap with {len(url_elements)} URL elements")
        found_fresh = False
        no_date_count = 0

        # Проверяем даты публикации
        for url_elem in url_elements:
            pub_date = self.extract_publication_date(url_elem, namespace)
            if pub_date is None:
                no_date_count += 1
                continue

            if pub_date >= self.min_date:
                # Если найдена хотя бы одна страница с датой не старше CHECK_LAST_MONTHS месяцев, считаем sitemap свежим
                found_fresh = True
                break

        # Если большинство страниц не имеют дат, это может указывать на проблему
        if no_date_count > len(url_elements) * 0.9 and len(url_elements) > 10:
            logger.warning(f"Sitemap has {no_date_count}/{len(url_elements)} URLs without dates, which may indicate issues")

        return found_fresh

    def is_sitemap_marked_as_not_fresh(self, sitemap_url: str) -> bool:
        """
        Проверяет, отмечен ли sitemap как не свежий (is_fresh=False) в базе данных.
        Возвращает True, если sitemap существует и помечен как не свежий.
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        "SELECT is_fresh, is_master FROM sitemaps WHERE sitemap_url = %s",
                        (sitemap_url,)
                    )
                    result = cursor.fetchone()

                    # Если записи нет, возвращаем False
                    if result is None:
                        return False

                    # Если это мастер-sitemap, считаем его всегда свежим
                    if result[1] is True:  # result[1] - это is_master
                        return False

                    # Если sitemap найден и is_fresh=False, возвращаем True
                    return result[0] is False

                except psycopg2.Error as e:
                    logger.error(f"Error checking sitemap freshness for {sitemap_url}: {e}")
                    return False

    def should_exclude_sitemap(self, url: str) -> bool:
        """
        Проверяет, должен ли sitemap быть исключен из обработки.

        Исключаются URL, которые:
        1. Оканчиваются на category-sitemap.xml
        2. Оканчиваются на author-sitemap.xml
        3. Содержат '/category/' в URL
        4. Содержат '/author/' в URL
        5. Уже отмечены как не свежие (is_fresh=False) в базе данных
        """
        # Проверка по шаблонам URL
        if url.endswith('category-sitemap.xml') or url.endswith('author-sitemap.xml'):
            logger.debug(f"Excluding sitemap (by extension): {url}")
            return True

        if '/category/' in url or '/author/' in url:
            logger.debug(f"Excluding sitemap (by path): {url}")
            return True

        # Проверка по флагу is_fresh в базе данных
        if self.is_sitemap_marked_as_not_fresh(url):
            logger.debug(f"Excluding sitemap (marked as not fresh in DB): {url}")
            return True

        return False

    def process_master_sitemap(self, sitemap_url: str) -> List[str]:
        """Обработать мастер-sitemap и вернуть список дочерних sitemap."""
        logger.info(f"Processing master sitemap: {sitemap_url}")
        domain = get_domain(sitemap_url)

        # Сохраняем мастер-sitemap в БД (всегда с is_fresh=True)
        master_sitemap = Sitemap(url=sitemap_url, is_master=True, is_fresh=True)
        master_id = self.save_sitemap(master_sitemap)

        # Получаем XML
        root = fetch_xml(sitemap_url)
        if root is None:
            logger.error(f"Failed to load master sitemap: {sitemap_url}")
            return []

        # Определяем, какое пространство имен используется в XML
        namespace = root.tag.split('}')[0] + '}' if '}' in root.tag else ''
        logger.debug(f"Master sitemap XML namespace: {namespace}")

        # Извлекаем ссылки на дочерние sitemap
        child_sitemaps = []

        # Проверяем разные варианты поиска sitemap элементов
        sitemap_elements = []
        search_paths = [
            # С пространством имен по умолчанию
            f".//{namespace}sitemap",
            # С префиксом sm
            ".//sm:sitemap",
            # Без пространства имен
            ".//sitemap"
        ]

        for path in search_paths:
            try:
                if path.startswith(".//sm:"):
                    elements = root.findall(path, self.namespaces)
                else:
                    elements = root.findall(path)

                if elements:
                    sitemap_elements = elements
                    logger.debug(f"Found sitemap elements using path: {path}")
                    break
            except Exception as e:
                logger.warning(f"Error searching with path {path}: {e}")

        for sitemap_elem in sitemap_elements:
            # Пытаемся найти URL с использованием различных путей
            loc_text = None
            loc_search_paths = [
                # С пространством имен по умолчанию
                f".//{namespace}loc",
                # С префиксом sm
                ".//sm:loc",
                # Без пространства имен
                ".//loc"
            ]

            for loc_path in loc_search_paths:
                try:
                    if loc_path.startswith(".//sm:"):
                        loc_elem = sitemap_elem.find(loc_path, self.namespaces)
                    else:
                        loc_elem = sitemap_elem.find(loc_path)

                    if loc_elem is not None and loc_elem.text:
                        loc_text = loc_elem.text.strip()
                        break
                except Exception as e:
                    continue

            if not loc_text:
                logger.warning("Could not find location (URL) for sitemap element")
                continue

            child_url = loc_text

            # Проверяем, нужно ли исключить этот sitemap
            # Сюда также входит проверка на is_fresh=False в БД
            if self.should_exclude_sitemap(child_url):
                logger.debug(f"Skipping excluded sitemap: {child_url}")
                continue

            # Ищем дату последнего изменения
            lastmod_text = None
            lastmod_search_paths = [
                # С пространством имен по умолчанию
                f".//{namespace}lastmod",
                # С префиксом sm
                ".//sm:lastmod",
                # Без пространства имен
                ".//lastmod"
            ]

            for lastmod_path in lastmod_search_paths:
                try:
                    if lastmod_path.startswith(".//sm:"):
                        lastmod_elem = sitemap_elem.find(lastmod_path, self.namespaces)
                    else:
                        lastmod_elem = sitemap_elem.find(lastmod_path)

                    if lastmod_elem is not None and lastmod_elem.text:
                        lastmod_text = lastmod_elem.text.strip()
                        break
                except Exception:
                    continue

            last_mod = None
            if lastmod_text:
                last_mod = parse_date(lastmod_text)

            # Сначала сохраняем дочерний sitemap без флага свежести (он будет проверен позже)
            child_sitemap = Sitemap(
                url=child_url,
                is_master=False,
                parent_id=master_id,
                last_mod=last_mod,
                is_fresh=False  # По умолчанию не свежий, будет проверен при обработке
            )
            child_id = self.save_sitemap(child_sitemap)

            # Добавляем URL в список для дальнейшей обработки
            # Свежесть sitemap будет определена при обработке
            child_sitemaps.append(child_url)

        logger.info(f"Found {len(child_sitemaps)} child sitemaps in {sitemap_url}")
        return child_sitemaps

    def extract_publication_date(self, url_elem, namespace='') -> Optional[datetime.datetime]:
        """Извлекает дату публикации из элемента URL различными способами."""
        # Находим URL страницы для логирования
        url = None

        # Пытаемся найти элемент loc разными способами
        loc_search_paths = [
            # С пространством имен по умолчанию
            f"./{namespace}loc",
            # С префиксом sm
            "./sm:loc",
            # Без пространства имен
            "./loc"
        ]

        for loc_path in loc_search_paths:
            try:
                if loc_path.startswith("./sm:"):
                    loc_elem = url_elem.find(loc_path, self.namespaces)
                else:
                    loc_elem = url_elem.find(loc_path)

                if loc_elem is not None and loc_elem.text:
                    url = loc_elem.text.strip()
                    break
            except Exception as e:
                continue

        # Проверяем различные пути для поиска дат публикации
        date_search_paths = [
            # lastmod с разными пространствами имен
            f"./{namespace}lastmod",
            "./sm:lastmod",
            "./lastmod",

            # news:publication_date с разными пространствами имен
            ".//news:news/news:publication_date",

            # Другие стандартные теги с датами
            ".//publication_date",
            ".//pubDate",
            ".//date",
            ".//updated",
            ".//modified",
            ".//published",
            ".//created",
            ".//issued",
            ".//time",

            # Dublin Core и Open Graph
            ".//dc:date",
            ".//article:published_time",

            # Атрибуты HTML meta
            ".//meta[@property='article:published_time']/@content",
            ".//meta[@name='date']/@content",
            ".//meta[@name='pubdate']/@content",
            ".//meta[@name='publish_date']/@content",
            ".//meta[@name='article:published_time']/@content",
            ".//meta[@itemprop='datePublished']/@content",

            # Атрибуты с датами
            ".//*[@date]/@date",
            ".//*[@datetime]/@datetime",
            ".//*[@pubdate]/@pubdate",
            ".//*[@published]/@published",
            ".//*[@created]/@created"
        ]

        # Проверяем каждый путь поиска
        for path in date_search_paths:
            try:
                if path.startswith(".//news:") or path.startswith("./sm:"):
                    date_elem = url_elem.find(path, self.namespaces)
                else:
                    date_elem = url_elem.find(path)

                if date_elem is not None:
                    date_text = date_elem.text if hasattr(date_elem, 'text') and date_elem.text else str(date_elem)
                    if date_text:
                        date = parse_date(date_text.strip())
                        if date:
                            return date
            except Exception:
                continue

        # Если дата публикации не найдена, логируем ошибку
        if url:
            logger.debug(f"Publication date not found for URL: {url}")
        else:
            logger.debug("Publication date not found for URL element (URL not found)")

        return None

    def process_child_sitemap(self, sitemap_url: str, parent_id: Optional[int] = None) -> int:
        """Обработать дочерний sitemap и извлечь URL страниц."""
        logger.info(f"Processing child sitemap: {sitemap_url}")

        # Проверяем, нужно ли исключить этот sitemap
        # включая проверку флага is_fresh=False в БД
        if self.should_exclude_sitemap(sitemap_url):
            logger.debug(f"Skipping excluded sitemap: {sitemap_url}")
            return 0

        domain = get_domain(sitemap_url)

        # Получаем XML перед сохранением sitemap
        root = fetch_xml(sitemap_url)
        if root is None:
            logger.error(f"Failed to load child sitemap: {sitemap_url}")
            return 0

        # Определяем, какое пространство имен используется в XML
        namespace = root.tag.split('}')[0] + '}' if '}' in root.tag else ''
        logger.debug(f"XML namespace: {namespace}")

        # Проверяем, существует ли этот домен в базе данных
        is_new_domain = self.is_new_domain(domain)

        # Для нового домена все sitemap считаются свежими
        # Для существующих доменов проверяем содержимое sitemap
        if is_new_domain:
            is_fresh = True
            logger.info(f"New domain detected: {domain}. Marking all sitemaps as fresh.")
        else:
            # Проверяем свежесть sitemap - есть ли в нем страницы с датой не старше CHECK_LAST_MONTHS месяцев
            is_fresh = self.check_sitemap_freshness(root)

        # Сохраняем дочерний sitemap в БД, если его еще нет
        if parent_id:
            child_sitemap = Sitemap(url=sitemap_url, is_master=False, parent_id=parent_id, is_fresh=is_fresh)
            sitemap_id = self.save_sitemap(child_sitemap)
        else:
            # Ищем существующий sitemap в БД
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT id_sitemap FROM sitemaps WHERE sitemap_url = %s",
                        (sitemap_url,)
                    )
                    result = cursor.fetchone()

                    if result:
                        sitemap_id = result[0]
                        # Обновляем флаг свежести
                        self.update_sitemap_freshness(sitemap_id, is_fresh)
                    else:
                        # Если не найден, создаем новый
                        child_sitemap = Sitemap(url=sitemap_url, is_master=False, is_fresh=is_fresh)
                        sitemap_id = self.save_sitemap(child_sitemap)

        # Если sitemap не свежий, пропускаем обработку URL
        if not is_fresh:
            months_ago = CHECK_LAST_MONTHS
            logger.info(f"Skipping outdated sitemap: {sitemap_url} (no news younger than {months_ago} months)")
            return 0

        # Извлекаем URL страниц
        page_count = 0
        skipped_no_date = 0

        # Проверяем разные варианты поиска URL элементов
        url_elements = []
        search_paths = [
            # С пространством имен по умолчанию
            f".//{namespace}url",
            # С префиксом sm
            ".//sm:url",
            # Без пространства имен
            ".//url"
        ]

        for path in search_paths:
            try:
                if path.startswith(".//sm:"):
                    elements = root.findall(path, self.namespaces)
                else:
                    elements = root.findall(path)

                if elements:
                    url_elements = elements
                    logger.debug(f"Found URL elements using path: {path}")
                    break
            except Exception as e:
                logger.warning(f"Error searching with path {path}: {e}")

        logger.info(f"Found {len(url_elements)} URL elements in {sitemap_url}")

        for url_elem in url_elements:
            # Пытаемся найти элемент loc разными способами
            loc_text = None
            loc_search_paths = [
                # С пространством имен по умолчанию
                f"./{namespace}loc",
                # С префиксом sm
                "./sm:loc",
                # Без пространства имен
                "./loc"
            ]

            for loc_path in loc_search_paths:
                try:
                    if loc_path.startswith("./sm:"):
                        loc_elem = url_elem.find(loc_path, self.namespaces)
                    else:
                        loc_elem = url_elem.find(loc_path)

                    if loc_elem is not None and loc_elem.text:
                        loc_text = loc_elem.text.strip()
                        break
                except Exception as e:
                    logger.warning(f"Error searching for loc with path {loc_path}: {e}")

            if not loc_text:
                logger.warning("Could not find location (URL) for URL element")
                continue

            page_url = loc_text

            # Извлекаем дату публикации разными способами
            publication_date = self.extract_publication_date(url_elem, namespace)

            # Пропускаем страницы без даты публикации
            if publication_date is None:
                skipped_no_date += 1
                logger.debug(f"Skipping page {page_url} - publication date not found")
                continue

            # Если это новый домен - сохраняем все страницы, независимо от даты
            # Для существующих доменов - пропускаем страницы с датой старше минимальной
            if not is_new_domain and publication_date < self.min_date:
                months_ago = CHECK_LAST_MONTHS
                logger.debug(f"Skipping page {page_url} - publication date is older than {months_ago} months")
                self.site_stats[domain].pages_skipped += 1
                continue

            # Логируем информацию о дате публикации для отладки
            logger.debug(f"Found publication date for {page_url}: {publication_date}")

            # Сохраняем страницу
            news_page = NewsPage(
                page_url=page_url,
                id_sitemap=sitemap_id,
                publication_date=publication_date,
                is_error=False  # По умолчанию отмечаем страницу как не содержащую ошибок
            )

            if self.save_news_page(news_page, domain):
                page_count += 1

        # Обновляем счетчик страниц без дат
        self.site_stats[domain].pages_no_date += skipped_no_date

        logger.info(f"Found {page_count} valid pages in {sitemap_url} (skipped {skipped_no_date} without dates)")
        return page_count

    def process_master_sitemaps_file(self, file_path: str):
        """Обработать файл с URL мастер-sitemap."""
        logger.info(f"Начинаю обработку файла с мастер-sitemap: {file_path}")

        total_child_sitemaps = 0
        total_pages = 0
        total_domains = 0
        failed_domains = []

        try:
            if not os.path.exists(file_path):
                logger.error(f"Файл {file_path} не найден!")
                return

            with open(file_path, 'r') as f:
                master_urls = [line.strip() for line in f if line.strip()]

            logger.info(f"Обнаружено {len(master_urls)} мастер-sitemap в файле")

            # Обрабатываем мастер-sitemaps
            for master_url in master_urls:
                domain = get_domain(master_url)
                total_domains += 1

                logger.info("=" * 40)
                logger.info(f"Обработка домена: {domain}")
                logger.info("=" * 40)

                try:
                    # Сбрасываем статистику для текущего домена
                    self.site_stats[domain] = SiteStats()

                    child_sitemaps = self.process_master_sitemap(master_url)
                    total_child_sitemaps += len(child_sitemaps)

                    # Фильтруем дочерние sitemap перед обработкой
                    filtered_child_sitemaps = []
                    excluded_count = 0

                    for url in child_sitemaps:
                        if self.should_exclude_sitemap(url):
                            excluded_count += 1
                            logger.debug(f"Исключаю sitemap из обработки: {url}")
                        else:
                            filtered_child_sitemaps.append(url)

                    if excluded_count > 0:
                        logger.info(f"Исключено {excluded_count} sitemap из обработки")

                    logger.info(f"Начинаю обработку {len(filtered_child_sitemaps)} дочерних sitemap для {domain}")

                    # Обрабатываем отфильтрованные дочерние sitemaps в многопоточном режиме
                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        future_to_url = {executor.submit(self.process_child_sitemap, url): url for url in filtered_child_sitemaps}

                        completed = 0
                        for future in as_completed(future_to_url):
                            url = future_to_url[future]
                            completed += 1
                            try:
                                page_count = future.result()
                                total_pages += page_count
                                logger.info(f"Прогресс: {completed}/{len(filtered_child_sitemaps)} sitemap обработано ({completed/len(filtered_child_sitemaps)*100:.1f}%)")
                            except Exception as e:
                                logger.error(f"Ошибка при обработке {url}: {e}")

                    # Выводим статистику по домену в рамке
                    print_stats_box(domain, self.site_stats[domain])

                except Exception as e:
                    logger.error(f"Ошибка при обработке домена {domain}: {e}", exc_info=True)
                    failed_domains.append(domain)

            # Итоговая статистика
            logger.info("=" * 80)
            logger.info("ИТОГОВАЯ СТАТИСТИКА")
            logger.info("-" * 80)
            logger.info(f"Обработано доменов: {total_domains}")
            logger.info(f"Обработано дочерних sitemap: {total_child_sitemaps}")
            logger.info(f"Найдено страниц: {total_pages}")

            if failed_domains:
                logger.error(f"Не удалось обработать следующие домены: {', '.join(failed_domains)}")

            logger.info("=" * 80)

            # Выводим таблицу с итоговой статистикой по доменам
            self.print_tabulated_stats()

        except Exception as e:
            logger.error(f"Ошибка при обработке файла {file_path}: {e}", exc_info=True)

    def print_tabulated_stats(self):
        """Вывести статистику по доменам в виде таблицы с использованием tabulate."""
        table_data = []

        for domain, stats in self.site_stats.items():
            # Форматируем даты как строки в формате YYYY-MM-DD
            date_from = stats.min_date.strftime("%Y-%m-%d") if stats.min_date else "-"
            date_to = stats.max_date.strftime("%Y-%m-%d") if stats.max_date else "-"

            # Формируем строку для таблицы
            row = [
                domain,
                stats.sitemaps_processed,
                stats.pages_added,
                stats.pages_skipped,
                date_from,
                date_to
            ]
            table_data.append(row)

        # Заголовки столбцов
        headers = ["Domain", "Sitemaps processed", "Pages added", "Pages skipped", "Date from", "Date to"]

        # Выводим таблицу
        print("\n" + "=" * 100)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        print("=" * 100 + "\n")

    def is_new_domain(self, domain: str) -> bool:
        """
        Проверяет, является ли домен новым (отсутствующим в базе данных).
        Возвращает True, если домен новый, иначе False.
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        "SELECT COUNT(*) FROM sitemaps WHERE domain = %s",
                        (domain,)
                    )
                    result = cursor.fetchone()

                    # Если количество записей > 0, то домен уже существует
                    return result[0] == 0
                except psycopg2.Error as e:
                    logger.error(f"Error checking if domain {domain} is new: {e}")
                    # В случае ошибки считаем домен существующим (более безопасное поведение)
                    return False

def main():
    """Основная функция программы."""
    start_time = time.time()

    logger.info("Запуск парсера sitemap")
    logger.info(f"Минимальная дата для новостей: {get_min_date().strftime('%Y-%m-%d')}")

    try:
        processor = SitemapProcessor()
        processor.process_master_sitemaps_file(SITEMAP_FILE_PATH)
    except Exception as e:
        logger.error(f"Критическая ошибка при выполнении программы: {e}", exc_info=True)
    finally:
        end_time = time.time()
        execution_time = end_time - start_time

        # Форматируем время выполнения в удобочитаемом виде
        hours, remainder = divmod(execution_time, 3600)
        minutes, seconds = divmod(remainder, 60)

        logger.info(f"Программа завершена. Время выполнения: {int(hours)}ч {int(minutes)}м {int(seconds)}с")

if __name__ == "__main__":
    main()