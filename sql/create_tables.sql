-- Таблица для хранения информации о sitemap файлах
CREATE TABLE IF NOT EXISTS sitemaps (
    id_sitemap SERIAL PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    sitemap_url TEXT NOT NULL UNIQUE,
    is_master BOOLEAN NOT NULL,
    parent_id INTEGER REFERENCES sitemaps(id_sitemap),
    last_mod TIMESTAMP WITH TIME ZONE,
    is_fresh BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Создаем индекс по URL sitemap для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_sitemaps_url ON sitemaps(sitemap_url);
CREATE INDEX IF NOT EXISTS idx_sitemaps_domain ON sitemaps(domain);
CREATE INDEX IF NOT EXISTS idx_sitemaps_fresh ON sitemaps(is_fresh);

-- Таблица для хранения информации о страницах новостей
CREATE TABLE IF NOT EXISTS news_pages (
    id_page SERIAL PRIMARY KEY,
    id_sitemap INTEGER NOT NULL REFERENCES sitemaps(id_sitemap),
    page_url TEXT NOT NULL UNIQUE,
    publication_date TIMESTAMP WITH TIME ZONE,
    is_error BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Создаем индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_news_pages_sitemap ON news_pages(id_sitemap);
CREATE INDEX IF NOT EXISTS idx_news_pages_url ON news_pages(page_url);
CREATE INDEX IF NOT EXISTS idx_news_pages_date ON news_pages(publication_date);

-- Создание таблицы для хранения контента новостных страниц
CREATE TABLE public.news_pages_content (
	id serial4 NOT NULL,
	id_page int4 NOT NULL,
	page_url TEXT NULL,
	publication_date TIMESTAMP WITH TIME ZONE NULL,
	page_raw_content text NULL,
	"content" text NULL,
	content_length integer NULL,
	fetched_at timestamptz DEFAULT now() NULL,
	CONSTRAINT news_pages_content_id_page_key UNIQUE (id_page),
	CONSTRAINT news_pages_content_pkey PRIMARY KEY (id),
	CONSTRAINT news_pages_content_id_page_fkey FOREIGN KEY (id_page) REFERENCES public.news_pages(id_page)
);

CREATE INDEX idx_news_pages_content_id_page ON public.news_pages_content USING btree (id_page);
CREATE INDEX idx_news_pages_content_page_url ON public.news_pages_content USING btree (page_url);
CREATE INDEX idx_news_pages_content_publication_date ON public.news_pages_content USING btree (publication_date);
CREATE INDEX idx_news_pages_content_length ON public.news_pages_content USING btree (content_length);

-- Создание расширения pgvector для работы с векторными эмбеддингами
CREATE EXTENSION IF NOT EXISTS vector;

-- Создание таблицы для хранения эмбеддингов контента
CREATE TABLE IF NOT EXISTS public.content_embeddings (
    id serial4 NOT NULL,
    id_page int4 NOT NULL,
    embedding vector(1536) NULL,
    CONSTRAINT content_embeddings_pkey PRIMARY KEY (id),
    CONSTRAINT content_embeddings_id_page_key UNIQUE (id_page),
    CONSTRAINT content_embeddings_id_page_fkey FOREIGN KEY (id_page) REFERENCES public.news_pages(id_page)
);

CREATE INDEX idx_content_embeddings_id_page ON public.content_embeddings USING btree (id_page);
