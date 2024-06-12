-- create table for company stock data
DROP TABLE IF EXISTS company_stock_prices;

CREATE TABLE company_stock_prices (
    id SERIAL ,
    company TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    price NUMERIC NOT NULL
);

SELECT create_hypertable('company_stock_prices', 'timestamp');

-- create table for target stock data
DROP TABLE IF EXISTS target_company_stock_prices;

CREATE TABLE target_company_stock_prices (
    id SERIAL ,
    company TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    price NUMERIC NOT NULL
);

SELECT create_hypertable('target_company_stock_prices', 'timestamp');