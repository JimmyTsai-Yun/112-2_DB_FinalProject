-- SELECT COUNT(*) 
-- FROM target_company_stock_prices
-- WHERE company = 'GOLD_WAR';

-- SELECT COUNT(*) 
-- FROM company_stock_prices
-- WHERE company = 'ABT'
-- LIMIT 10;

DROP TABLE IF EXISTS company_dtw_results;

CREATE TABLE company_dtw_results (
    company TEXT,
    best_index INT,
    min_dtw DOUBLE PRECISION
);

DO $$
DECLARE
    companies TEXT[] := ARRAY['ABT', 'ACN', 'ADBE', 'AMD', 'AMGN', 'AMZN', 'APPL', 'AVGO', 'BAC', 'BRK', 'CAT', 'CORN', 'COST', 'CRM', 'CSCO', 'cvx', 'DIS', 'FED RATE', 'GDP', 'GE', 'GOOGL', 'HD', 'INTU', 'IOEU4', 'JNJ', 'jpm us equity', 'KO', 'LLY', 'LMAHD', 'LMCAD', 'MA', 'MCD', 'MSFT', 'NFLX', 'NVDA', 'ORCL', 'PEP', 'PG', 'PFE', 'QCOM', 'TSLA', 'UNH', 'US PPI', 'V', 'VZ', 'WGC', 'WMT', 'WTI', 'XOM'];
    companyname TEXT;
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    index INT;
    min_d DOUBLE PRECISION;
    company_start_index INT;
BEGIN

    SELECT ARRAY_AGG(price)
    INTO short_array
    FROM (SELECT price FROM target_company_stock_prices WHERE company = 'GOLD_WAR') AS subquery;

    -- 遍歷公司列表
    FOR i IN 1..array_length(companies, 1) LOOP
        companyname := companies[i];
        
        -- 使用 ARRAY_AGG 聚合函數暫存查詢結果
        SELECT ARRAY_AGG(price)
        INTO long_array
        FROM (SELECT price FROM company_stock_prices WHERE company = companyname) AS subquery;

        SELECT id
        INTO company_start_index
        FROM company_stock_prices
        WHERE company = companyname
        LIMIT 1;

        -- 調用 find_min_dtw_subarray 函數並傳入存儲的陣列
        FOR index, min_d IN 
            SELECT best_index, min_dtw 
            FROM find_min_dtw_subarray(long_array, short_array)
        LOOP
            -- 打印結果
            INSERT INTO company_dtw_results (company, best_index, min_dtw)
            VALUES (companyname, index+company_start_index-1, min_d);
            RAISE NOTICE 'Company: %, Best Index: %, Min DTW: %, start index: %', companyname, index, min_d, index+company_start_index-1;
        END LOOP;
    END LOOP;
END $$;

SELECT * FROM company_dtw_results;

-- DO $$
-- DECLARE
--     companies TEXT[] := ARRAY['ABT', 'ACN', 'ADBE', 'AMD', 'AMGN', 'AMZN', 'APPL', 'AVGO', 'BAC', 'BRK', 'CAT', 'CORN', 'COST', 'CRM', 'CSCO', 'cvx', 'DIS', 'FED RATE', 'GDP', 'GE', 'GOOGL', 'HD', 'INTU', 'IOEU4', 'JNJ', 'jpm us equity', 'KO', 'LLY', 'LMAHD', 'LMCAD', 'MA', 'MCD', 'MSFT', 'NFLX', 'NVDA', 'ORCL', 'PEP', 'PG', 'PFE', 'QCOM', 'TSLA', 'UNH', 'US PPI', 'V', 'VZ', 'WGC', 'WMT', 'WTI', 'XOM'];
--     companyname TEXT;
--     long_array DOUBLE PRECISION[];
--     short_array DOUBLE PRECISION[];
--     index INT;
--     min_d DOUBLE PRECISION;
-- BEGIN

--     SELECT ARRAY_AGG(price)
--     INTO short_array
--     FROM (SELECT price FROM target_company_stock_prices WHERE company = 'GOLD_WAR') AS subquery;

--     -- 遍歷公司列表
--     FOR i IN 1..array_length(companies, 1) LOOP
--         companyname := companies[i];
        
--         -- 使用 ARRAY_AGG 聚合函數暫存查詢結果
--         SELECT ARRAY_AGG(price)
--         INTO long_array
--         FROM (SELECT price FROM company_stock_prices WHERE company = companyname) AS subquery;

--         -- 調用 find_min_dtw_subarray 函數並傳入存儲的陣列
--         FOR index, min_d IN 
--             SELECT best_index, min_dtw 
--             FROM find_min_dtw_subarray(long_array, short_array)
--         LOOP
--             -- 打印結果
--             RAISE NOTICE 'Company: %, Best Index: %, Min DTW: %', companyname, index, min_d;
--         END LOOP;
--     END LOOP;
-- END $$;


-- APPL Pure Euclidean distance


