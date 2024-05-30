DROP TABLE IF EXISTS fromch_vsift_results;

CREATE TABLE fromch_vsift_results (
    company TEXT,
    best_index INT,
    min_dis DOUBLE PRECISION,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

DO $$
DECLARE
    companies TEXT[] := ARRAY['ABT', 'ACN', 'ADBE', 'AMD', 'AMGN', 'AMZN', 'APPL', 'AVGO', 'BAC', 'BRK', 'CAT', 'CORN', 'COST', 'CRM', 'CSCO', 'cvx', 'DIS', 'FED RATE', 'GDP', 'GE', 'GOOGL', 'HD', 'INTU', 'IOEU4', 'JNJ', 'jpm us equity', 'KO', 'LLY', 'LMAHD', 'LMCAD', 'MA', 'MCD', 'MSFT', 'NFLX', 'NVDA', 'ORCL', 'PEP', 'PG', 'PFE', 'QCOM', 'TSLA', 'UNH', 'US PPI', 'V', 'VZ', 'WGC', 'WMT', 'WTI', 'XOM'];
    -- companies TEXT[] := ARRAY['ABT', 'ACN'];
    companyname TEXT;
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    index INT;
    min_d DOUBLE PRECISION;
BEGIN

    SELECT ARRAY_AGG(price)
    INTO short_array
    FROM (SELECT price FROM target_company_stock_prices WHERE company = 'IMPORTCN') AS subquery;

    -- 遍歷公司列表
    FOR i IN 1..array_length(companies, 1) LOOP
        companyname := companies[i];
        
        -- 使用 ARRAY_AGG 聚合函數暫存查詢結果
        SELECT ARRAY_AGG(avg_price)
        INTO long_array
        FROM (
            SELECT AVG(price) AS avg_price
            FROM company_stock_prices
            WHERE company = companyname
            GROUP BY time_bucket('1 month', timestamp)
            ORDER BY time_bucket('1 month', timestamp)
        ) AS subquery;

        -- 調用 find_min_dtw_subarray 函數並傳入存儲的陣列
        FOR index, min_d IN 
            SELECT best_index, min_vshift_euclidean 
            FROM find_min_vshift_euclidean_subarray(long_array, short_array)
        LOOP
            -- 打印結果
            INSERT INTO fromch_vsift_results (company, best_index, min_dis)
            VALUES (companyname, index, min_d);
            RAISE NOTICE 'Company: %, Best Index: %, Min DTW: %', companyname, index, min_d;
        END LOOP;
    END LOOP;
END $$;

SELECT * FROM fromch_vsift_results;





-- -- 創建新表
-- DROP TABLE IF EXISTS monthly_avg_prices;

-- CREATE TABLE monthly_avg_prices (
--     index INT,
--     month TIMESTAMP,
--     avg_price DOUBLE PRECISION
-- );

-- -- 插入查詢結果
-- INSERT INTO monthly_avg_prices (index, month, avg_price)
-- SELECT 
--     ROW_NUMBER() OVER (ORDER BY month) AS index,
--     month,
--     avg_price
-- FROM (
--     SELECT 
--         time_bucket('1 month', timestamp) AS month, 
--         AVG(price) AS avg_price
--     FROM 
--         company_stock_prices
--     WHERE 
--         company = 'AMD'
--     GROUP BY 
--         time_bucket('1 month', timestamp)
--     ORDER BY 
--         time_bucket('1 month', timestamp)
-- ) AS subquery;

-- -- 驗證插入結果
-- SELECT * FROM monthly_avg_prices;

