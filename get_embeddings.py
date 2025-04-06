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

    def connect_to_db(self):
        """Создание подключения к БД"""
        return psycopg2.connect(**self.db_config)

    def get_unprocessed_contents(self):
        """Получение контента, для которого еще не созданы эмбеддинги"""
        with self.connect_to_db() as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT npc.id_page, npc.content
                FROM news_pages_content npc
                LEFT JOIN content_embeddings ce ON npc.id_page = ce.id_page
                WHERE ce.id IS NULL AND npc.content IS NOT NULL AND LENGTH(npc.content) > 200
                """
                cursor.execute(query)
                return cursor.fetchall()

    async def get_embedding(self, text, id_page):
        """
        Получение эмбеддинга для текста

        :param text: Текст для получения эмбеддинга
        :param id_page: Идентификатор страницы
        :return: Кортеж (id_page, embedding)
        """
        async with self.semaphore:
            for attempt in range(MAX_RETRIES):
                try:
                    response = await self.client.embeddings.create(
                        model=self.openai_config['model_embeddings'],
                        input=text,
                        encoding_format="float"
                    )
                    embedding = response.data[0].embedding
                    return id_page, embedding
                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(f"Ошибка при получении эмбеддинга для id_page={id_page}. Попытка {attempt + 1}/{MAX_RETRIES}. Ошибка: {e}")
                        await asyncio.sleep(RETRY_DELAY * (2 ** attempt))  # Экспоненциальная задержка
                    else:
                        logger.error(f"Не удалось получить эмбеддинг для id_page={id_page} после {MAX_RETRIES} попыток. Ошибка: {e}")
                        raise

    async def process_batch(self, batch, pbar):
        """
        Обработка пакета контента

        :param batch: Список кортежей (id_page, content) для обработки
        :param pbar: Прогресс-бар
        :return: Список результатов (id_page, embedding)
        """
        tasks = []
        for id_page, content in batch:
            if content:  # Проверка, что контент не пустой
                tasks.append(self.get_embedding(content, id_page))
            else:
                logger.warning(f"Пропуск id_page={id_page} из-за пустого контента")
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

def main():
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

if __name__ == "__main__":
    main()