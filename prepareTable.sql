-- DROP TABLE IF EXISTS company_stock_prices;

-- CREATE TABLE company_stock_prices (
--     id SERIAL ,
--     company TEXT NOT NULL,
--     timestamp TIMESTAMPTZ NOT NULL,
--     price NUMERIC NOT NULL
-- );

-- SELECT create_hypertable('company_stock_prices', 'timestamp');

-- SELECT * FROM company_stock_prices
-- LIMIT 10;

-- DROP TABLE IF EXISTS target_company_stock_prices;

-- CREATE TABLE target_company_stock_prices (
--     id SERIAL ,
--     company TEXT NOT NULL,
--     timestamp TIMESTAMPTZ NOT NULL,
--     price NUMERIC NOT NULL
-- );

-- SELECT create_hypertable('target_company_stock_prices', 'timestamp');

SELECT * FROM target_company_stock_prices
WHERE company = 'GOLD_WAR';

-- SELECT array_agg(price ORDER BY timestamp DESC, price DESC) AS prices_array
-- FROM (
--     SELECT price, timestamp
--     FROM company_stock_prices
--     WHERE company = 'TSLA'
--     ORDER BY timestamp DESC, price DESC
--     LIMIT 10
-- ) s;

-- SELECT dtw_distance(ARRAY[1,3,4], ARRAY[7,9]);

-- WITH precomputed_prices AS (
--     SELECT
--         company,
--         array_agg(price ORDER BY timestamp) AS agg_prices
--     FROM
--         company_stock_prices
--     GROUP BY company
--     LIMIT 10
-- )
-- SELECT
--     a.company
--     -- dtw_distance(a.agg_prices, b.agg_prices) AS price_distance
-- FROM
--     precomputed_prices a,
--     precomputed_prices b
-- WHERE
--     a.company = 'AAPL' AND b.company = 'TSLA';

-- SELECT 
--         company,
--         array_agg(price ORDER BY timestamp DESC, price DESC) AS prices_array
--     FROM (
--         SELECT company, price, timestamp
--         FROM company_stock_prices
--         ORDER BY timestamp DESC, price DESC
--         LIMIT 1000000000
--     ) s
--     GROUP BY company;

WITH precomputed_prices AS (
    SELECT 
        company,
        array_agg(price ORDER BY timestamp DESC, price DESC) AS prices_array
    FROM (
        SELECT company, price, timestamp
        FROM company_stock_prices
        ORDER BY timestamp DESC, price DESC
        LIMIT 10
    ) s
    GROUP BY company
)
SELECT 
    a.company as a_company,
    b.company as b_company,
    dtw_distance(a.prices_array, b.prices_array) AS price_distance
FROM 
    precomputed_prices a,
    precomputed_prices b
WHERE 
    a.company = 'TSLA' AND b.company = 'AAPL';



