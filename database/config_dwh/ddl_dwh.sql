-- Tạo SEQUENCE cho các bảng
CREATE SEQUENCE IF NOT EXISTS company_id_seq;
CREATE SEQUENCE IF NOT EXISTS topic_id_seq;
CREATE SEQUENCE IF NOT EXISTS new_id_seq;
CREATE SEQUENCE IF NOT EXISTS candle_id_seq;
CREATE SEQUENCE IF NOT EXISTS new_company_id_seq;
CREATE SEQUENCE IF NOT EXISTS new_topic_id_seq;
CREATE SEQUENCE IF NOT EXISTS time_id_seq;

-- dim_time table
CREATE TABLE IF NOT EXISTS dim_time (
    id INTEGER DEFAULT NEXTVAL('time_id_seq') PRIMARY KEY,
    date DATE NOT NULL,
    day_of_week VARCHAR(10),
    month VARCHAR(10),
    quarter VARCHAR(10),
    year INTEGER
);

-- dim_companies table
CREATE TABLE IF NOT EXISTS dim_companies (
    id INTEGER DEFAULT NEXTVAL('company_id_seq') PRIMARY KEY,
    name VARCHAR(255),
    ticker VARCHAR(10) NOT NULL,
    is_delisted BOOLEAN NOT NULL,
    category VARCHAR(255),
    currency VARCHAR(10),
    location VARCHAR(255),
    exchange VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    industry VARCHAR(255),
    sector VARCHAR(255),
    sic_industry VARCHAR(255),
    sic_sector VARCHAR(255),
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);

-- dim_topics table
CREATE TABLE IF NOT EXISTS dim_topics (
    id INTEGER DEFAULT NEXTVAL('topic_id_seq') PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    CONSTRAINT unique_topic UNIQUE (name)
);

-- dim_news table
CREATE TABLE IF NOT EXISTS dim_news (
    id INTEGER DEFAULT NEXTVAL('new_id_seq') PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    time_published CHAR(15) NOT NULL,
    authors VARCHAR[],
    summary TEXT,
    source TEXT,
    overall_sentiment_score DOUBLE NOT NULL,
    overall_sentiment_label VARCHAR(255) NOT NULL,
    time_id INTEGER,
    FOREIGN KEY (time_id) REFERENCES dim_time(id)
);

-- fact_candles table
CREATE TABLE IF NOT EXISTS fact_candles (
    id INTEGER DEFAULT NEXTVAL('candle_id_seq') PRIMARY KEY,
    company_id INTEGER NOT NULL,
    volume INTEGER NOT NULL,
    volume_weighted DOUBLE NOT NULL,
    open DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    time_stamp CHAR(15) NOT NULL,
    num_of_trades INTEGER NOT NULL,
    is_otc BOOLEAN DEFAULT false,
    time_id INTEGER,
    FOREIGN KEY (company_id) REFERENCES dim_companies(id),
    FOREIGN KEY (time_id) REFERENCES dim_time(id)
);

-- fact_news_companies table
CREATE TABLE IF NOT EXISTS fact_news_companies (
    id INTEGER DEFAULT NEXTVAL('new_company_id_seq') PRIMARY KEY,
    company_id INTEGER NOT NULL,
    new_id INTEGER NOT NULL,
    relevance_score DOUBLE NOT NULL,
    ticker_sentiment_score DOUBLE NOT NULL,
    ticker_sentiment_label VARCHAR(100) NOT NULL,
    FOREIGN KEY (company_id) REFERENCES dim_companies(id),
    FOREIGN KEY (new_id) REFERENCES dim_news(id)
);

-- fact_news_topics table
CREATE TABLE IF NOT EXISTS fact_news_topics (
    id INTEGER DEFAULT NEXTVAL('new_topic_id_seq') PRIMARY KEY,
    new_id INTEGER NOT NULL,
    topic_id INTEGER NOT NULL,
    relevance_score DOUBLE NOT NULL,
    FOREIGN KEY (new_id) REFERENCES dim_news(id),
    FOREIGN KEY (topic_id) REFERENCES dim_topics(id)
);