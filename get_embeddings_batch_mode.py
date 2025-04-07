#!/usr/bin/env python
import os
import sys
import time
import asyncio
import json
import psycopg2
import argparse
from psycopg2.extras import execute_values
from openai import AsyncOpenAI, OpenAI
from dotenv import load_dotenv
from tqdm import tqdm
import logging
import configparser
from tabulate import tabulate
from datetime import datetime
import tiktoken
import tempfile

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Отключение логов HTTP-запросов от OpenAI
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Загрузка переменных окружения
load_dotenv()

# Константы
BATCH_SIZE = 100  # Количество текстов, обрабатываемых за один раз
MAX_RETRIES = 3
RETRY_DELAY = 2  # Задержка между повторными попытками в секундах
CONCURRENCY_LIMIT = 5  # Лимит параллельных запросов к API
CONFIG_PATH = "config/config.ini"
CONFIG_SECTION_DB = "RSS-News.postgres_local"
CONFIG_SECTION_OPENAI = "OpenAI"
CONTENT_MIN_LENGTH = 500
TOTAL_LIMIT = 1000
PRICE_PER_1M_TOKENS = 0.02 # цена за 1 млн токенов
BATCH_CHECK_INTERVAL = 10  # Интервал проверки статуса батча в секундах
BATCH_MAX_WAIT_TIME = 24 * 60 * 60  # Максимальное время ожидания результатов батча (24 часа)
BATCH_INFO_FILE = "batch_jobs.json"  # Файл для хранения информации о batch-заданиях

def load_config():
    """Загрузка конфигурации из файла"""
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    db_config = {
        'host': config[CONFIG_SECTION_DB]['host'],
        'port': config[CONFIG_SECTION_DB]['port'],
        'dbname': config[CONFIG_SECTION_DB]['dbname'],
        'user': config[CONFIG_SECTION_DB]['user'],
        'password': config[CONFIG_SECTION_DB]['password']
    }

    openai_config = {
        'api_key': config[CONFIG_SECTION_OPENAI]['api_key'],
        'model_embeddings': config[CONFIG_SECTION_OPENAI]['model_embeddings']
    }

    return db_config, openai_config

class EmbeddingProcessor:
    def __init__(self, db_config, openai_config):
        """
        Инициализация процессора эмбеддингов

        :param db_config: Словарь с параметрами подключения к БД
        :param openai_config: Словарь с параметрами OpenAI
        """
        self.db_config = db_config
        self.openai_config = openai_config
        self.async_client = AsyncOpenAI(api_key=openai_config['api_key'])
        self.sync_client = OpenAI(api_key=openai_config['api_key'])
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

        # Инициализация токенизатора для модели
        # Используем соответствующий токенизатор для модели эмбеддингов
        self.tokenizer = tiktoken.encoding_for_model(openai_config['model_embeddings'])
        if not hasattr(tiktoken.model, openai_config['model_embeddings']):
            # Если точная модель не найдена, используем cl100k_base (для text-embedding-3-*)
            self.tokenizer = tiktoken.get_encoding("cl100k_base")

        # Статистика сессии
        self.stats = {
            'total_texts': 0,
            'processed_texts': 0,
            'successful_embeddings': 0,
            'failed_embeddings': 0,
            'skipped_texts': 0,
            'start_time': datetime.now(),
            'domains_stats': {},
            'tokens_sent': 0,
            'avg_tokens_per_text': 0,
            'batch_jobs': 0,
            'batch_processing_time': 0
        }

        # Информация о batch-заданиях для отслеживания
        self.batch_jobs = self.load_batch_jobs()

    def connect_to_db(self):
        """Создание подключения к БД"""
        return psycopg2.connect(**self.db_config)

    def load_batch_jobs(self):
        """Загрузка информации о batch-заданиях из файла"""
        if os.path.exists(BATCH_INFO_FILE):
            try:
                with open(BATCH_INFO_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Ошибка при загрузке информации о batch-заданиях: {e}")
                return {'pending': [], 'completed': [], 'failed': []}
        else:
            return {'pending': [], 'completed': [], 'failed': []}

    def save_batch_jobs(self):
        """Сохранение информации о batch-заданиях в файл"""
        try:
            with open(BATCH_INFO_FILE, 'w') as f:
                json.dump(self.batch_jobs, f, indent=2)
            logger.info(f"Информация о batch-заданиях сохранена в {BATCH_INFO_FILE}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении информации о batch-заданиях: {e}")

    def get_unprocessed_contents(self):
        """Получение контента, для которого еще не созданы эмбеддинги"""
        # Получение ID страниц, для которых уже созданы batch-задания
        pending_ids = []
        for job in self.batch_jobs['pending']:
            pending_ids.extend(job.get('page_ids', []))

        with self.connect_to_db() as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT npc.id_page, npc.content, s.domain
                FROM news_pages_content npc
                LEFT JOIN content_embeddings ce ON npc.id_page = ce.id_page
                JOIN news_pages np ON npc.id_page = np.id_page
                JOIN sitemaps s ON np.id_sitemap = s.id_sitemap
                WHERE ce.id IS NULL
                  AND npc.content IS NOT NULL
                  AND LENGTH(npc.content) >= %s
                  AND npc.id_page NOT IN (
                    SELECT UNNEST(%s::bigint[])
                  )
                ORDER BY npc.publication_date DESC
                LIMIT %s
                """
                cursor.execute(query, (CONTENT_MIN_LENGTH, pending_ids if pending_ids else [0], TOTAL_LIMIT))
                return cursor.fetchall()

    def prepare_batch_file(self, contents):
        """
        Подготовка JSONL файла для Batch API

        :param contents: Список кортежей (id_page, content, domain)
        :return: Путь к созданному JSONL файлу и словарь с токенами по доменам
        """
        fd, temp_path = tempfile.mkstemp(suffix='.jsonl')

        # Словарь для хранения информации о токенах по доменам
        domain_tokens = {}
        total_tokens = 0
        page_ids = []

        with os.fdopen(fd, 'w') as f:
            for i, (id_page, content, domain) in enumerate(contents):
                if not content:
                    logger.warning(f"Пропуск id_page={id_page} из-за пустого контента")
                    self.stats['skipped_texts'] += 1
                    if domain not in self.stats['domains_stats']:
                        self.stats['domains_stats'][domain] = {'success': 0, 'failed': 0, 'skipped': 0, 'tokens': 0}
                    self.stats['domains_stats'][domain]['skipped'] += 1
                    continue

                # Сохраняем ID страницы для отслеживания
                page_ids.append(id_page)

                # Подсчет токенов в тексте
                tokens = self.tokenizer.encode(content)
                token_count = len(tokens)
                total_tokens += token_count

                # Обновление статистики токенов по доменам
                if domain not in domain_tokens:
                    domain_tokens[domain] = 0
                domain_tokens[domain] += token_count

                # Создание запроса для Batch API
                request = {
                    "custom_id": str(id_page),
                    "method": "POST",
                    "url": "/v1/embeddings",
                    "body": {
                        "model": self.openai_config['model_embeddings'],
                        "input": content,
                        "encoding_format": "float"
                    }
                }

                f.write(json.dumps(request) + '\n')
                self.stats['processed_texts'] += 1

        logger.info(f"Создан JSONL файл с {self.stats['processed_texts']} запросами, всего токенов: {total_tokens}")
        self.stats['tokens_sent'] += total_tokens

        return temp_path, domain_tokens, page_ids

    def upload_batch_file(self, file_path):
        """
        Загрузка JSONL файла через Files API

        :param file_path: Путь к JSONL файлу
        :return: ID загруженного файла
        """
        try:
            with open(file_path, 'rb') as file:
                response = self.sync_client.files.create(
                    file=file,
                    purpose="batch"
                )
            logger.info(f"Файл успешно загружен, ID: {response.id}")
            return response.id
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла: {e}")
            raise

    def create_batch_job(self, file_id):
        """
        Создание batch-задания

        :param file_id: ID загруженного файла
        :return: ID созданного batch-задания
        """
        try:
            response = self.sync_client.batches.create(
                input_file_id=file_id,
                endpoint="/v1/embeddings",
                completion_window="24h"
            )
            logger.info(f"Batch-задание создано, ID: {response.id}, статус: {response.status}")
            self.stats['batch_jobs'] += 1
            return response.id
        except Exception as e:
            logger.error(f"Ошибка при создании batch-задания: {e}")
            raise

    def wait_for_batch_completion(self, batch_id):
        """
        Ожидание завершения batch-задания с периодической проверкой статуса

        :param batch_id: ID batch-задания
        :return: Объект batch с результатами
        """
        start_time = time.time()
        wait_time = 0

        logger.info(f"Ожидание завершения batch-задания {batch_id}...")

        while wait_time < BATCH_MAX_WAIT_TIME:
            batch = self.sync_client.batches.retrieve(batch_id)

            if batch.status in ["completed", "failed", "expired", "cancelled"]:
                end_time = time.time()
                processing_time = end_time - start_time
                self.stats['batch_processing_time'] += processing_time

                logger.info(f"Batch-задание {batch_id} завершено со статусом {batch.status} за {processing_time:.2f} секунд")

                if batch.status == "completed":
                    logger.info(f"Обработано запросов: {batch.request_counts.completed}/{batch.request_counts.total}")
                elif batch.status in ["failed", "expired", "cancelled"]:
                    logger.error(f"Batch-задание завершилось с ошибкой. Статус: {batch.status}")
                    if batch.status == "failed" and batch.error_file_id:
                        error_content = self.sync_client.files.content(batch.error_file_id)
                        logger.error(f"Ошибки: {error_content.text[:500]}...")

                return batch

            wait_time = time.time() - start_time
            logger.info(f"Ожидание... Текущий статус: {batch.status}, прошло {wait_time:.2f} секунд")
            time.sleep(BATCH_CHECK_INTERVAL)

        # Если превышено максимальное время ожидания
        logger.error(f"Превышено максимальное время ожидания ({BATCH_MAX_WAIT_TIME} секунд) для batch-задания {batch_id}")
        return None

    def check_batch_status(self, batch_id):
        """
        Проверка статуса batch-задания без ожидания

        :param batch_id: ID batch-задания
        :return: Статус задания и объект batch
        """
        try:
            batch = self.sync_client.batches.retrieve(batch_id)
            return batch.status, batch
        except Exception as e:
            logger.error(f"Ошибка при проверке статуса batch-задания {batch_id}: {e}")
            return "error", None

    def process_batch_results(self, batch, domain_tokens):
        """
        Обработка результатов batch-задания и сохранение эмбеддингов в БД

        :param batch: Объект batch с результатами
        :param domain_tokens: Словарь с токенами по доменам
        :return: Количество успешно обработанных эмбеддингов
        """
        if not batch or not batch.output_file_id:
            logger.error("Нет результатов для обработки")
            return 0

        try:
            # Получение результатов
            output_content = self.sync_client.files.content(batch.output_file_id)
            output_text = output_content.text

            # Обработка ошибок, если они есть
            error_data = {}
            if batch.error_file_id:
                error_content = self.sync_client.files.content(batch.error_file_id)
                for line in error_content.text.strip().split('\n'):
                    error_obj = json.loads(line)
                    error_data[error_obj['custom_id']] = error_obj['error']

            # Парсинг результатов
            embeddings = []
            successful_count = 0
            failed_count = 0

            # Обработка каждой строки результатов
            with self.connect_to_db() as conn:
                with conn.cursor() as cursor:
                    for line in output_text.strip().split('\n'):
                        result = json.loads(line)
                        custom_id = result['custom_id']
                        id_page = int(custom_id)

                        # Проверка наличия ошибки
                        if result.get('error') or result['response']['status_code'] != 200:
                            failed_count += 1
                            logger.error(f"Ошибка при получении эмбеддинга для id_page={id_page}: {result.get('error')}")
                            continue

                        # Извлечение эмбеддинга
                        response_body = result['response']['body']
                        embedding = response_body['data'][0]['embedding']

                        # Добавление в список для пакетной вставки
                        embeddings.append((id_page, embedding))
                        successful_count += 1

                        # Пакетная вставка каждые 100 эмбеддингов для экономии памяти
                        if len(embeddings) >= 100:
                            execute_values(
                                cursor,
                                "INSERT INTO content_embeddings (id_page, embedding) VALUES %s ON CONFLICT (id_page) DO UPDATE SET embedding = EXCLUDED.embedding",
                                embeddings,
                                template="(%s, %s)"
                            )
                            conn.commit()
                            embeddings = []

                    # Вставка оставшихся эмбеддингов
                    if embeddings:
                        execute_values(
                            cursor,
                            "INSERT INTO content_embeddings (id_page, embedding) VALUES %s ON CONFLICT (id_page) DO UPDATE SET embedding = EXCLUDED.embedding",
                            embeddings,
                            template="(%s, %s)"
                        )
                        conn.commit()

            # Обновление статистики
            self.stats['successful_embeddings'] += successful_count
            self.stats['failed_embeddings'] += failed_count

            # Обновление статистики по доменам
            for domain, tokens in domain_tokens.items():
                if domain not in self.stats['domains_stats']:
                    self.stats['domains_stats'][domain] = {'success': 0, 'failed': 0, 'skipped': 0, 'tokens': 0}

                # Пропорционально распределяем успехи и неудачи по доменам на основе их доли токенов
                domain_ratio = tokens / sum(domain_tokens.values()) if sum(domain_tokens.values()) > 0 else 0
                domain_success = round(successful_count * domain_ratio)
                domain_failed = round(failed_count * domain_ratio)

                self.stats['domains_stats'][domain]['success'] += domain_success
                self.stats['domains_stats'][domain]['failed'] += domain_failed
                self.stats['domains_stats'][domain]['tokens'] += tokens

            logger.info(f"Обработано эмбеддингов: {successful_count} успешно, {failed_count} с ошибками")
            return successful_count

        except Exception as e:
            logger.error(f"Ошибка при обработке результатов: {e}")
            return 0

    async def create_batches(self):
        """
        Создание batch-заданий без ожидания результатов
        """
        contents = self.get_unprocessed_contents()
        total = len(contents)
        self.stats['total_texts'] = total

        if total == 0:
            logger.info("Все тексты уже обработаны или находятся в очереди на обработку")
            return

        logger.info(f"Найдено {total} текстов для обработки через Batch API")

        # Разбиение на пакеты
        for i in range(0, total, BATCH_SIZE):
            batch_contents = contents[i:i+BATCH_SIZE]
            logger.info(f"Создание пакета {i//BATCH_SIZE + 1}/{(total+BATCH_SIZE-1)//BATCH_SIZE} ({len(batch_contents)} текстов)")

            try:
                # Шаг 1: Подготовка JSONL файла
                batch_file_path, domain_tokens, page_ids = self.prepare_batch_file(batch_contents)

                # Шаг 2: Загрузка файла
                file_id = self.upload_batch_file(batch_file_path)

                # Шаг 3: Создание batch-задания
                batch_id = self.create_batch_job(file_id)

                # Сохранение информации о задании
                job_info = {
                    'batch_id': batch_id,
                    'input_file_id': file_id,
                    'created_at': datetime.now().isoformat(),
                    'status': 'pending',
                    'page_ids': page_ids,
                    'domain_tokens': domain_tokens
                }

                self.batch_jobs['pending'].append(job_info)
                self.save_batch_jobs()

                # Очистка временного файла
                try:
                    os.remove(batch_file_path)
                except Exception as e:
                    logger.warning(f"Не удалось удалить временный файл {batch_file_path}: {e}")

            except Exception as e:
                logger.error(f"Ошибка при создании batch-задания: {e}")

        logger.info(f"Создано {self.stats['batch_jobs']} batch-заданий")

    async def check_pending_batches(self):
        """
        Проверка статуса ожидающих batch-заданий и обработка завершенных
        """
        if not self.batch_jobs['pending']:
            logger.info("Нет ожидающих batch-заданий для проверки")
            return

        logger.info(f"Проверка статуса {len(self.batch_jobs['pending'])} ожидающих batch-заданий")

        # Создаем копию списка, т.к. будем его изменять
        pending_jobs = self.batch_jobs['pending'].copy()

        for job in pending_jobs:
            batch_id = job['batch_id']
            status, batch = self.check_batch_status(batch_id)

            logger.info(f"Статус задания {batch_id}: {status}")

            if status in ["completed", "failed", "expired", "cancelled"]:
                # Обновляем информацию о задании
                job['status'] = status
                job['completed_at'] = datetime.now().isoformat()

                # Обрабатываем результаты для завершенных заданий
                if status == "completed":
                    result_count = self.process_batch_results(batch, job['domain_tokens'])
                    job['processed_count'] = result_count
                    self.batch_jobs['completed'].append(job)
                else:
                    self.batch_jobs['failed'].append(job)

                # Удаляем из списка ожидающих
                self.batch_jobs['pending'].remove(job)

                logger.info(f"Задание {batch_id} перемещено в список {status}")

        # Сохраняем обновленную информацию
        self.save_batch_jobs()

        logger.info(f"Проверка завершена. Осталось ожидающих заданий: {len(self.batch_jobs['pending'])}")

    async def process_all(self, mode='full'):
        """
        Основной метод обработки в зависимости от режима работы

        :param mode: Режим работы ('full', 'create' или 'check')
        """
        if mode == 'create':
            # Только создание batch-заданий
            await self.create_batches()
        elif mode == 'check':
            # Только проверка и обработка существующих заданий
            await self.check_pending_batches()
        else:
            # Полный цикл: создание и проверка
            await self.create_batches()
            await self.check_pending_batches()

    async def process_batch(self, contents):
        """
        Обработка пакета контента через Batch API в режиме ожидания
        (устаревший метод, оставлен для обратной совместимости)

        :param contents: Список кортежей (id_page, content, domain) для обработки
        :return: Количество успешно обработанных эмбеддингов
        """
        if not contents:
            logger.info("Нет данных для обработки")
            return 0

        try:
            # Шаг 1: Подготовка JSONL файла
            batch_file_path, domain_tokens, _ = self.prepare_batch_file(contents)

            # Шаг 2: Загрузка файла
            file_id = self.upload_batch_file(batch_file_path)

            # Шаг 3: Создание batch-задания
            batch_id = self.create_batch_job(file_id)

            # Шаг 4: Ожидание завершения
            batch = self.wait_for_batch_completion(batch_id)

            # Шаг 5: Обработка результатов
            result_count = self.process_batch_results(batch, domain_tokens)

            # Очистка временного файла
            try:
                os.remove(batch_file_path)
            except Exception as e:
                logger.warning(f"Не удалось удалить временный файл {batch_file_path}: {e}")

            return result_count

        except Exception as e:
            logger.error(f"Ошибка при обработке пакета через Batch API: {e}")
            return 0

    async def get_embedding(self, text, id_page, domain):
        """
        Получение эмбеддинга для текста (метод оставлен для обратной совместимости)

        :param text: Текст для получения эмбеддинга
        :param id_page: Идентификатор страницы
        :param domain: Домен страницы
        :return: Кортеж (id_page, embedding)
        """
        async with self.semaphore:
            # Подсчет токенов в тексте
            tokens = self.tokenizer.encode(text)
            token_count = len(tokens)
            self.stats['tokens_sent'] += token_count

            for attempt in range(MAX_RETRIES):
                try:
                    response = await self.async_client.embeddings.create(
                        model=self.openai_config['model_embeddings'],
                        input=text,
                        encoding_format="float"
                    )
                    embedding = response.data[0].embedding

                    # Обновление статистики успешной обработки
                    self.stats['successful_embeddings'] += 1
                    if domain not in self.stats['domains_stats']:
                        self.stats['domains_stats'][domain] = {'success': 0, 'failed': 0, 'skipped': 0, 'tokens': 0}
                    self.stats['domains_stats'][domain]['success'] += 1
                    self.stats['domains_stats'][domain]['tokens'] = self.stats['domains_stats'][domain].get('tokens', 0) + token_count

                    return id_page, embedding
                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(f"Ошибка при получении эмбеддинга для id_page={id_page}. Попытка {attempt + 1}/{MAX_RETRIES}. Ошибка: {e}")
                        await asyncio.sleep(RETRY_DELAY * (2 ** attempt))  # Экспоненциальная задержка
                    else:
                        logger.error(f"Не удалось получить эмбеддинг для id_page={id_page} после {MAX_RETRIES} попыток. Ошибка: {e}")

                        # Обновление статистики неудачной обработки
                        self.stats['failed_embeddings'] += 1
                        if domain not in self.stats['domains_stats']:
                            self.stats['domains_stats'][domain] = {'success': 0, 'failed': 0, 'skipped': 0, 'tokens': 0}
                        self.stats['domains_stats'][domain]['failed'] += 1

                        raise

    def save_embeddings(self, embeddings):
        """
        Сохранение эмбеддингов в БД

        :param embeddings: Список кортежей (id_page, embedding)
        """
        if not embeddings:
            return

        with self.connect_to_db() as conn:
            with conn.cursor() as cursor:
                # Подготовка данных для вставки
                values = [(id_page, embedding) for id_page, embedding in embeddings]

                # Вставка данных с использованием execute_values для эффективности
                execute_values(
                    cursor,
                    "INSERT INTO content_embeddings (id_page, embedding) VALUES %s ON CONFLICT (id_page) DO UPDATE SET embedding = EXCLUDED.embedding",
                    values,
                    template="(%s, %s)"
                )

            conn.commit()

    def print_stats(self):
        """Вывод статистики сессии обработки эмбеддингов"""
        end_time = datetime.now()
        processing_time = (end_time - self.stats['start_time']).total_seconds()

        # Расчет средних токенов и стоимости
        if self.stats['successful_embeddings'] > 0:
            self.stats['avg_tokens_per_text'] = self.stats['tokens_sent'] / self.stats['successful_embeddings']
        else:
            self.stats['avg_tokens_per_text'] = 0

        # Стоимость в долларах (с учетом 50% скидки для Batch API)
        cost = (self.stats['tokens_sent'] / 1_000_000) * PRICE_PER_1M_TOKENS * 0.5  # 50% скидка для Batch API

        # Общая статистика
        summary_stats = [
            ["Всего текстов для обработки", self.stats['total_texts']],
            ["Успешно созданных эмбеддингов", self.stats['successful_embeddings']],
            ["Неудачных попыток", self.stats['failed_embeddings']],
            ["Пропущенных текстов", self.stats['skipped_texts']],
            ["Всего отправлено токенов", f"{self.stats['tokens_sent']:,}".replace(',', ' ')],
            ["Среднее кол-во токенов на текст", f"{self.stats['avg_tokens_per_text']:.1f}"],
            ["Стоимость API (с 50% скидкой Batch API)", f"${cost:.6f}"],
            ["Количество batch-заданий", self.stats['batch_jobs']],
            ["Время обработки batch-заданий", f"{self.stats['batch_processing_time']:.2f} сек"],
            ["Общее время обработки", f"{processing_time:.2f} сек"],
            ["Скорость", f"{self.stats['successful_embeddings'] / max(processing_time, 1):.2f} эмбеддингов/сек"]
        ]

        print("\n📊 СТАТИСТИКА СЕССИИ СОЗДАНИЯ ЭМБЕДДИНГОВ")
        print(tabulate(summary_stats, headers=["Метрика", "Значение"], tablefmt="pretty"))

        # Статистика по доменам
        if self.stats['domains_stats']:
            domains_table = []
            for i, (domain, stats) in enumerate(sorted(self.stats['domains_stats'].items()), 1):
                domain_tokens = stats.get('tokens', 0)
                domain_cost = (domain_tokens / 1_000_000) * PRICE_PER_1M_TOKENS * 0.5  # 50% скидка для Batch API
                domains_table.append([
                    i, domain,
                    stats.get('success', 0),
                    stats.get('failed', 0),
                    stats.get('skipped', 0),
                    stats.get('success', 0) + stats.get('failed', 0) + stats.get('skipped', 0),
                    f"{domain_tokens:,}".replace(',', ' '),
                    f"${domain_cost:.6f}"
                ])

            print("\n📈 СТАТИСТИКА ПО ДОМЕНАМ")
            print(tabulate(
                domains_table,
                headers=["№", "Домен", "Успешно", "Ошибок", "Пропущено", "Всего", "Токенов", "Стоимость"],
                tablefmt="pretty",
                colalign=("right", "left", "right", "right", "right", "right", "right", "right")
            ))

    def print_batch_status(self):
        """Вывод статистики по batch-заданиям"""
        if not any([self.batch_jobs['pending'], self.batch_jobs['completed'], self.batch_jobs['failed']]):
            print("\nНет информации о batch-заданиях")
            return

        # Статистика по статусам
        status_counts = {
            'pending': len(self.batch_jobs['pending']),
            'completed': len(self.batch_jobs['completed']),
            'failed': len(self.batch_jobs['failed'])
        }

        status_table = []
        for status, count in status_counts.items():
            status_table.append([status, count])

        print("\n📊 СТАТУС BATCH-ЗАДАНИЙ")
        print(tabulate(status_table, headers=["Статус", "Количество"], tablefmt="pretty"))

        # Список ожидающих заданий
        if self.batch_jobs['pending']:
            pending_table = []
            for i, job in enumerate(self.batch_jobs['pending'], 1):
                created_at = datetime.fromisoformat(job['created_at'])
                elapsed = (datetime.now() - created_at).total_seconds() / 3600  # в часах
                pending_table.append([
                    i,
                    job['batch_id'],
                    f"{elapsed:.1f} ч",
                    len(job['page_ids'])
                ])

            print("\n⏳ ОЖИДАЮЩИЕ BATCH-ЗАДАНИЯ")
            print(tabulate(
                pending_table,
                headers=["№", "ID задания", "Время в обработке", "Кол-во страниц"],
                tablefmt="pretty"
            ))

def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Создание эмбеддингов для текстов через Batch API OpenAI')
    parser.add_argument('--mode', type=str, choices=['full', 'create', 'check', 'status'], default='full',
                        help='Режим работы: full (полный цикл), create (только создание заданий), '
                             'check (проверка и обработка результатов), status (вывод текущего статуса)')
    return parser.parse_args()

def main():
    args = parse_args()
    start_time = time.time()
    processor = None

    try:
        # Загрузка конфигурации из файла
        db_config, openai_config = load_config()

        # Создание процессора эмбеддингов
        processor = EmbeddingProcessor(db_config, openai_config)

        if args.mode == 'status':
            # Только вывод статуса batch-заданий
            processor.print_batch_status()
        else:
            # Запуск обработки в выбранном режиме
            asyncio.run(processor.process_all(args.mode))
            logger.info(f"Обработка в режиме '{args.mode}' завершена успешно")

            # Вывод статистики
            processor.print_stats()
            processor.print_batch_status()

    except KeyboardInterrupt:
        logger.info("Обработка прервана пользователем")
    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}", exc_info=True)
    finally:
        # Сохранение информации о batch-заданиях, если был создан процессор
        if processor:
            processor.save_batch_jobs()

if __name__ == "__main__":
    main()
