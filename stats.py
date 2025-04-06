#!/usr/bin/env python
import os
import configparser
import psycopg2
from tabulate import tabulate
from datetime import datetime

# Константы
CONFIG_PATH = "config/config.ini"
CONFIG_SECTION_DB = "RSS-News.postgres_local"

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

    return db_config

def connect_to_db(db_config):
    """Создание подключения к БД"""
    return psycopg2.connect(**db_config)

def execute_query(conn, query):
    """Выполнение SQL-запроса и получение результатов"""
    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        return columns, results

def show_monthly_stats(conn):
    """Отображение ежемесячной статистики контента по доменам"""
    query = """
    SELECT
        s.domain,
        date_trunc('month', npc.publication_date)::date AS month,
        COUNT(*) AS content_count,
        ROUND(SUM(npc.content_length)::numeric / (1024 * 1024), 4) AS total_content_mb,
        ROUND(AVG(npc.content_length)::numeric, 2) AS avg_content_bytes
    FROM sitemaps s
    JOIN news_pages np ON s.id_sitemap = np.id_sitemap
    JOIN news_pages_content npc ON np.id_page = npc.id_page
    WHERE npc.content IS NOT NULL
      AND npc.publication_date >= date_trunc('month', current_date) - interval '5 months'
    GROUP BY s.domain, month
    ORDER BY s.domain, month DESC;
    """

    columns, results = execute_query(conn, query)

    # Добавляем нумерацию и меняем названия колонок
    numbered_results = []
    for i, row in enumerate(results, 1):
        numbered_results.append([i] + list(row))

    headers = ["№", "Домен", "Месяц", "Количество\nконтента", "Общий\nобъем\n(МБ)", "Средний\nразмер\n(байт)"]
    colalign = ["right", "left", "right", "right", "right", "right"]

    print("\n📊 ПОМЕСЯЧНАЯ СТАТИСТИКА КОНТЕНТА ПО ДОМЕНАМ")
    print(tabulate(numbered_results, headers=headers, tablefmt="pretty", colalign=colalign))

def show_domain_stats(conn):
    """Отображение общей статистики страниц по доменам"""
    query = """
    SELECT
        s.domain,
        COUNT(np.id_page) AS total_pages,
        COUNT(npc.id_page) AS pages_in_news_pages_content,
        COUNT(CASE WHEN npc.content IS NOT NULL AND npc.content <> '' THEN 1 END) AS pages_with_non_empty_content,
        COUNT(DISTINCT ce.id_page) AS pages_with_embeddings,
        MIN(np.publication_date)::date AS min_publication_date,
        MAX(np.publication_date)::date AS max_publication_date
    FROM sitemaps s
    JOIN news_pages np ON s.id_sitemap = np.id_sitemap
    LEFT JOIN news_pages_content npc ON np.id_page = npc.id_page
    LEFT JOIN content_embeddings ce ON np.id_page = ce.id_page
    GROUP BY s.domain
    ORDER BY s.domain;
    """

    columns, results = execute_query(conn, query)

    # Добавляем нумерацию и меняем названия колонок
    numbered_results = []
    for i, row in enumerate(results, 1):
        numbered_results.append([i] + list(row))

    headers = ["№", "Домен", "Всего\nстраниц", "Страниц\nспарсено",
               "Страниц\nс контентом", "Страниц\nс эмбеддингами",
               "Миним.\nдата", "Максим.\nдата"]
    colalign = ["right", "left", "right", "right", "right", "right", "right", "right"]

    print("\n📈 ОБЩАЯ СТАТИСТИКА СТРАНИЦ ПО ДОМЕНАМ")
    print(tabulate(numbered_results, headers=headers, tablefmt="pretty", colalign=colalign))

def main():
    """Основная функция"""
    try:
        # Загрузка конфигурации
        db_config = load_config()

        # Подключение к БД
        with connect_to_db(db_config) as conn:
            # Вывод статистики
            show_monthly_stats(conn)
            show_domain_stats(conn)
    except Exception as e:
        print(f"\n❌ Ошибка при выполнении: {e}")

if __name__ == "__main__":
    main()