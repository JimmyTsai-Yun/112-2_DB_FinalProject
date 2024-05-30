-- DROP TABLE IF EXISTS gold_vsift_v2_results;

-- CREATE TABLE gold_vsift_v2_results (
--     company TEXT,
--     best_index INT,
--     min_dis DOUBLE PRECISION,
--     start_time TIMESTAMP,
--     end_time TIMESTAMP
-- );

-- DO $$
-- DECLARE
--     companies TEXT[] := ARRAY['ABT', 'ACN', 'ADBE', 'AMD', 'AMGN', 'AMZN', 'APPL', 'AVGO', 'BAC', 'BRK', 'CAT', 'CORN', 'COST', 'CRM', 'CSCO', 'cvx', 'DIS', 'FED RATE', 'GDP', 'GE', 'GOOGL', 'HD', 'INTU', 'IOEU4', 'JNJ', 'jpm us equity', 'KO', 'LLY', 'LMAHD', 'LMCAD', 'MA', 'MCD', 'MSFT', 'NFLX', 'NVDA', 'ORCL', 'PEP', 'PG', 'PFE', 'QCOM', 'TSLA', 'UNH', 'US PPI', 'V', 'VZ', 'WGC', 'WMT', 'WTI', 'XOM'];
--     -- companies TEXT[] := ARRAY['ABT', 'ACN'];
--     companyname TEXT;
--     long_array DOUBLE PRECISION[];
--     short_array DOUBLE PRECISION[];
--     index INT;
--     min_d DOUBLE PRECISION;
--     company_start_index INT;
-- BEGIN

--     SELECT ARRAY_AGG(price)
--     INTO short_array
--     FROM (SELECT price FROM target_company_stock_prices WHERE company = 'GOLD') AS subquery;

--     -- 遍歷公司列表
--     FOR i IN 1..array_length(companies, 1) LOOP
--         companyname := companies[i];
        
--         -- 使用 ARRAY_AGG 聚合函數暫存查詢結果，使用 time_bucket_gapfill 填補缺失值
--         SELECT ARRAY_AGG(price)
--         INTO long_array
--         FROM (
--             SELECT 
--                 time_bucket_gapfill('1 day', timestamp) AS bucket,
--                 LOCF(last(price, 'timestamp')) AS price
--             FROM company_stock_prices 
--             WHERE company = companyname
--               AND timestamp >= '2017-01-01'
--               AND timestamp < '2022-01-01'
--             GROUP BY bucket
--             ORDER BY bucket
--         ) AS subquery;

--         SELECT id
--         INTO company_start_index
--         FROM company_stock_prices
--         WHERE company = companyname
--             AND timestamp >= '2017-01-01'
--             AND timestamp < '2022-01-01'
--         LIMIT 1;

--         -- 調用 find_min_dtw_subarray 函數並傳入存儲的陣列
--         FOR index, min_d IN 
--             SELECT best_index, min_vshift_euclidean 
--             FROM find_min_vshift_euclidean_subarray(long_array, short_array)
--         LOOP
--             -- 打印結果
--             INSERT INTO gold_vsift_v2_results (company, best_index, min_dis)
--             VALUES (companyname, index+company_start_index-1, min_d);
--             RAISE NOTICE 'Company: %, Best Index: %, Min DTW: %, start index: %', companyname, index, min_d, index+company_start_index-1;
--         END LOOP;
--     END LOOP;
-- END $$;

-- UPDATE gold_vsift_v2_results
-- SET start_time = cs.timestamp
-- FROM company_stock_prices cs
-- WHERE gold_vsift_v2_results.company = cs.company
--   AND gold_vsift_v2_results.best_index = cs.id;

-- UPDATE gold_vsift_v2_results
-- SET end_time = cs.timestamp
-- FROM company_stock_prices cs
-- WHERE gold_vsift_v2_results.company = cs.company
--   AND (gold_vsift_v2_results.best_index+1304) = cs.id;

-- SELECT * FROM gold_vsift_v2_results;

-- SELECT 
--     time_bucket_gapfill('1 day', timestamp) AS bucket,
--     interpolate(avg(price)) AS price

-- FROM company_stock_prices 
-- WHERE company = 'ABT'
--   AND timestamp > '2017-01-03'::timestamp
--   AND timestamp < '2022-01-01'::timestamp
-- GROUP BY bucket
-- ORDER BY bucket;

-- WITH initial_fill AS (
--     SELECT 
--         time_bucket_gapfill('1 day', timestamp) AS bucket,
--         LOCF(last(price, timestamp)) AS price
--     FROM company_stock_prices 
--     WHERE company = 'ABT'
--       AND timestamp >= '2017-01-01'::timestamp
--       AND timestamp < '2022-01-01'::timestamp
--     GROUP BY bucket
-- )
-- SELECT
--     bucket,
--     interpolate(price) AS price
-- FROM initial_fill
-- ORDER BY bucket;

with filled_data as (
    SELECT 
        time_bucket_gapfill('1 day', timestamp, '2017-01-01'::timestamp, '2022-01-01'::timestamp) AS bucket,
        LOCF(last(price, timestamp)) AS price
    FROM company_stock_prices 
    WHERE company = 'ABT'
    AND timestamp > '2017-01-03'::timestamp  
    AND timestamp < '2022-01-01'::timestamp  
    GROUP BY bucket
    ORDER BY bucket
)
select count(*) from filled_data;

-- select count(*) from target_company_stock_prices where company = 'GOLD';

WITH filled_data AS (
    SELECT 
        time_bucket_gapfill('1 day', timestamp, '2017-01-01'::timestamp, '2022-01-01'::timestamp) AS bucket,
        LOCF(last(price, timestamp)) AS price
    FROM target_company_stock_prices 
    WHERE company = 'GOLD'
      AND timestamp > '2017-01-03'::timestamp
      AND timestamp < '2022-01-01'::timestamp
    GROUP BY bucket
    ORDER BY bucket
)
SELECT COUNT(*) FROM filled_data;

SELECT 
        time_bucket_gapfill('1 day', timestamp, '2017-01-01'::timestamp, '2022-01-01'::timestamp) AS bucket,
        LOCF(last(price, timestamp)) AS price
    FROM company_stock_prices 
    WHERE company = 'ABT'
    AND timestamp > '2017-01-03'::timestamp  
    AND timestamp < '2022-01-01'::timestamp  
    GROUP BY bucket
    ORDER BY bucket;

WITH filled_data AS (
    SELECT 
        time_bucket_gapfill('1 day', timestamp, '2017-01-04'::timestamp, '2022-01-01'::timestamp) AS bucket,
        LOCF(last(price, timestamp)) AS price
    FROM target_company_stock_prices 
    WHERE company = 'GOLD'
      AND timestamp > '2017-01-03'::timestamp
      AND timestamp < '2022-01-01'::timestamp
    GROUP BY bucket
)
SELECT 
    bucket,
    price
FROM filled_data
WHERE bucket >= '2017-01-01'::timestamp
  AND bucket < '2022-01-01'::timestamp
ORDER BY bucket;
