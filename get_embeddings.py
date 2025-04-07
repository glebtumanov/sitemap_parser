#!/usr/bin/env python
import os
import time
import asyncio
import psycopg2
from psycopg2.extras import execute_values
from openai import AsyncOpenAI
from dotenv import load_dotenv
from tqdm import tqdm
import logging
import configparser
from tabulate import tabulate
from datetime import datetime
import tiktoken

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
MAX_CONTENT_LENGTH = 8192 # Максимальная длина контента в токенах

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
        self.client = AsyncOpenAI(api_key=openai_config['api_key'])
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
            'chars_sent': 0,
            'avg_tokens_per_text': 0,
            'avg_chars_per_token': 0,
            'truncated_texts': 0
        }

    def connect_to_db(self):
        """Создание подключения к БД"""
        return psycopg2.connect(**self.db_config)

    def get_unprocessed_contents(self):
        """Получение контента, для которого еще не созданы эмбеддинги"""
        with self.connect_to_db() as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT npc.id_page, npc.content, s.domain
                FROM news_pages_content npc
                LEFT JOIN content_embeddings ce ON npc.id_page = ce.id_page
                JOIN news_pages np ON npc.id_page = np.id_page
                JOIN sitemaps s ON np.id_sitemap = s.id_sitemap
                WHERE ce.id IS NULL AND npc.content IS NOT NULL AND LENGTH(npc.content) >= %s
                ORDER BY npc.publication_date DESC
                LIMIT %s
                """
                cursor.execute(query, (CONTENT_MIN_LENGTH, TOTAL_LIMIT))
                return cursor.fetchall()

    def truncate_text_to_token_limit(self, text):
        """
        Обрезает текст до указанного количества токенов

        :param text: Исходный текст
        :return: Обрезанный текст, количество токенов, длина в символах
        """
        tokens = self.tokenizer.encode(text)
        original_token_count = len(tokens)

        # Проверяем, нужно ли обрезать
        if original_token_count <= MAX_CONTENT_LENGTH - 1:
            return text, original_token_count, len(text)

        # Обрезаем до MAX_CONTENT_LENGTH - 1 токенов
        truncated_tokens = tokens[:MAX_CONTENT_LENGTH - 1]
        truncated_text = self.tokenizer.decode(truncated_tokens)

        self.stats['truncated_texts'] += 1

        return truncated_text, len(truncated_tokens), len(truncated_text)

    async def get_embedding(self, text, id_page, domain):
        """
        Получение эмбеддинга для текста

        :param text: Текст для получения эмбеддинга
        :param id_page: Идентификатор страницы
        :param domain: Домен страницы
        :return: Кортеж (id_page, embedding)
        """
        async with self.semaphore:
            # Обрезаем текст до максимально допустимого количества токенов
            processed_text, token_count, char_count = self.truncate_text_to_token_limit(text)

            self.stats['tokens_sent'] += token_count
            self.stats['chars_sent'] += char_count

            for attempt in range(MAX_RETRIES):
                try:
                    response = await self.client.embeddings.create(
                        model=self.openai_config['model_embeddings'],
                        input=processed_text,
                        encoding_format="float"
                    )
                    embedding = response.data[0].embedding

                    # Обновление статистики успешной обработки
                    self.stats['successful_embeddings'] += 1
                    if domain not in self.stats['domains_stats']:
                        self.stats['domains_stats'][domain] = {'success': 0, 'failed': 0, 'skipped': 0, 'tokens': 0, 'chars': 0}
                    self.stats['domains_stats'][domain]['success'] += 1
                    self.stats['domains_stats'][domain]['tokens'] = self.stats['domains_stats'][domain].get('tokens', 0) + token_count
                    self.stats['domains_stats'][domain]['chars'] = self.stats['domains_stats'][domain].get('chars', 0) + char_count

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
                            self.stats['domains_stats'][domain] = {'success': 0, 'failed': 0, 'skipped': 0, 'tokens': 0, 'chars': 0}
                        self.stats['domains_stats'][domain]['failed'] += 1

                        raise

    async def process_batch(self, batch, pbar):
        """
        Обработка пакета контента

        :param batch: Список кортежей (id_page, content, domain) для обработки
        :param pbar: Прогресс-бар
        :return: Список результатов (id_page, embedding)
        """
        tasks = []
        for id_page, content, domain in batch:
            if content:  # Проверка, что контент не пустой
                tasks.append(self.get_embedding(content, id_page, domain))
                self.stats['processed_texts'] += 1
            else:
                logger.warning(f"Пропуск id_page={id_page} из-за пустого контента")
                self.stats['skipped_texts'] += 1
                # Обновление статистики для домена
                if domain not in self.stats['domains_stats']:
                    self.stats['domains_stats'][domain] = {'success': 0, 'failed': 0, 'skipped': 0, 'tokens': 0, 'chars': 0}
                self.stats['domains_stats'][domain]['skipped'] += 1
                pbar.update(1)

        results = []
        for future in asyncio.as_completed(tasks):
            try:
                result = await future
                results.append(result)
                pbar.update(1)
            except Exception as e:
                logger.error(f"Ошибка при обработке запроса: {e}")
                pbar.update(1)

        return results

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

    async def process_all(self):
        """Обработка всего контента"""
        contents = self.get_unprocessed_contents()
        total = len(contents)
        self.stats['total_texts'] = total

        if total == 0:
            logger.info("Все тексты уже обработаны")
            return

        logger.info(f"Найдено {total} текстов для обработки")

        with tqdm(total=total, desc="Получение эмбеддингов") as pbar:
            # Разбиение на пакеты
            for i in range(0, total, BATCH_SIZE):
                batch = contents[i:i+BATCH_SIZE]
                embeddings = await self.process_batch(batch, pbar)

                # Сохранение результатов пакета
                self.save_embeddings(embeddings)

    def print_stats(self):
        """Вывод статистики сессии обработки эмбеддингов"""
        end_time = datetime.now()
        processing_time = (end_time - self.stats['start_time']).total_seconds()

        # Расчет средних токенов, символов и стоимости
        if self.stats['successful_embeddings'] > 0:
            self.stats['avg_tokens_per_text'] = self.stats['tokens_sent'] / self.stats['successful_embeddings']
        else:
            self.stats['avg_tokens_per_text'] = 0

        if self.stats['tokens_sent'] > 0:
            self.stats['avg_chars_per_token'] = self.stats['chars_sent'] / self.stats['tokens_sent']
        else:
            self.stats['avg_chars_per_token'] = 0

        # Стоимость в долларах
        cost = (self.stats['tokens_sent'] / 1_000_000) * PRICE_PER_1M_TOKENS

        # Общая статистика
        summary_stats = [
            ["Всего текстов для обработки", self.stats['total_texts']],
            ["Успешно созданных эмбеддингов", self.stats['successful_embeddings']],
            ["Неудачных попыток", self.stats['failed_embeddings']],
            ["Пропущенных текстов", self.stats['skipped_texts']],
            ["Обрезанных текстов", self.stats['truncated_texts']],
            ["Всего отправлено токенов", f"{self.stats['tokens_sent']:,}".replace(',', ' ')],
            ["Всего отправлено символов", f"{self.stats['chars_sent']:,}".replace(',', ' ')],
            ["Среднее кол-во токенов на текст", f"{self.stats['avg_tokens_per_text']:.1f}"],
            ["Среднее кол-во символов на 1 токен", f"{self.stats['avg_chars_per_token']:.1f}"],
            ["Стоимость API", f"${cost:.6f}"],
            ["Время обработки", f"{processing_time:.2f} сек"],
            ["Скорость", f"{self.stats['successful_embeddings'] / max(processing_time, 1):.2f} эмбеддингов/сек"]
        ]

        print("\n📊 СТАТИСТИКА СЕССИИ СОЗДАНИЯ ЭМБЕДДИНГОВ")
        print(tabulate(summary_stats, headers=["Метрика", "Значение"], tablefmt="pretty",
                      colalign=("left", "right")))

        # Статистика по доменам
        if self.stats['domains_stats']:
            domains_table = []
            for i, (domain, stats) in enumerate(sorted(self.stats['domains_stats'].items()), 1):
                domain_tokens = stats.get('tokens', 0)
                domain_chars = stats.get('chars', 0)
                domain_cost = (domain_tokens / 1_000_000) * PRICE_PER_1M_TOKENS
                domains_table.append([
                    i, domain,
                    stats.get('success', 0),
                    stats.get('failed', 0),
                    stats.get('skipped', 0),
                    stats.get('success', 0) + stats.get('failed', 0) + stats.get('skipped', 0),
                    f"{domain_tokens:,}".replace(',', ' '),
                    f"{domain_chars:,}".replace(',', ' '),
                    f"${domain_cost:.6f}"
                ])

            print("\n📈 СТАТИСТИКА ПО ДОМЕНАМ")
            print(tabulate(
                domains_table,
                headers=["№", "Домен", "Успешно", "Ошибок", "Пропущено", "Всего", "Токенов", "Символов", "Стоимость"],
                tablefmt="pretty",
                colalign=("right", "left", "right", "right", "right", "right", "right", "right", "right")
            ))

def main():
    start_time = time.time()
    processor = None

    try:
        # Загрузка конфигурации из файла
        db_config, openai_config = load_config()

        # Создание и запуск процессора эмбеддингов
        processor = EmbeddingProcessor(db_config, openai_config)
        asyncio.run(processor.process_all())

        logger.info("Обработка завершена успешно")
    except KeyboardInterrupt:
        logger.info("Обработка прервана пользователем")
    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}", exc_info=True)
    finally:
        # Вывод статистики, если был создан процессор
        if processor:
            processor.print_stats()

if __name__ == "__main__":
    main()