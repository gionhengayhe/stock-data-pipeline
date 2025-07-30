CREATE TABLE IF NOT EXISTS regions (
    id SERIAL PRIMARY KEY,
    region VARCHAR(50) UNIQUE NOT NULL,
    local_open TIME NOT NULL,
    local_close TIME NOT NULL,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    hash_row TEXT GENERATED ALWAYS AS (
        md5(region || local_open::text || local_close::text)
    ) STORED
);

CREATE TABLE IF NOT EXISTS industries (
    id SERIAL PRIMARY KEY,
    industry VARCHAR(255) NOT NULL,
    sector VARCHAR(255) NOT NULL,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_industry UNIQUE (industry, sector)
);

CREATE TABLE IF NOT EXISTS sic_industries (
    id SERIAL PRIMARY KEY,
    sic_industry VARCHAR(255) NOT NULL,
    sic_sector VARCHAR(255) NOT NULL,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_sic_industry UNIQUE (sic_industry, sic_sector)
);

CREATE TABLE IF NOT EXISTS exchanges (
    id SERIAL PRIMARY KEY,
    region_id INT NOT NULL,
    name VARCHAR(100) UNIQUE NOT NULL,
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    hash_row TEXT GENERATED ALWAYS AS (
        md5(region_id::text || name)
    ) STORED,
    CONSTRAINT fk_exchange_region_id
        FOREIGN KEY(region_id)
        REFERENCES regions(id)
);

CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    exchange_id INT NOT NULL,
    industry_id INT,
    sic_id INT,
    name VARCHAR(255) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    is_delisted BOOLEAN NOT NULL,
    category VARCHAR(255),
    currency VARCHAR(10),
    location VARCHAR(255),
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    hash_row TEXT GENERATED ALWAYS AS (
        md5(exchange_id::text || industry_id::text || sic_id::text ||
            name || ticker || is_delisted::text || category || currency || location)
    ) STORED,
    CONSTRAINT fk_company_region
        FOREIGN KEY(exchange_id)
        REFERENCES exchanges(id),
    CONSTRAINT fk_company_industry_id
        FOREIGN KEY(industry_id)
        REFERENCES industries(id),
    CONSTRAINT fk_company_sic_id
        FOREIGN KEY(sic_id)
        REFERENCES sic_industries(id),
    CONSTRAINT unique_company_delisted UNIQUE (ticker, is_delisted)
);

