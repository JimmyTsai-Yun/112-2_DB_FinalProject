DROP TABLE IF EXISTS wmt_euclidean_results;

CREATE TABLE wmt_euclidean_results (
    company TEXT,
    best_index INT,
    min_dis DOUBLE PRECISION,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

DO $$
DECLARE
    companies TEXT[] := ARRAY['WGC', 'QCOM', 'ABT', 'AVGO', 'LMAHD', 'AMGN', 'IOEU4', 'BRK', 'GE', 'DIS', 'jpm us equity', 'CORN', 'WMT', 'NFLX', 'JNJ', 'MA', 'CRM', 'V', 'PFE', 'VZ', 'UNH', 'cvx', 'TSLA', 'PEP', 'LLY', 'WTI', 'INTU', 'ORCL', 'GOOGL', 'AMZN', 'NVDA', 'MSFT', 'BAC', 'LMCAD', 'AMD', 'HD', 'ACN', 'USDCNY', 'XOM', 'CSCO', 'DOLLAR', 'MCD', 'CAT', 'APPL', 'KO', 'PG', 'ADBE', 'GOLD', 'COST', 'TXN'];
    -- companies TEXT[] := ARRAY['ABT', 'ACN'];
    companyname TEXT;
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    index INT;
    min_d DOUBLE PRECISION;
    company_start_index INT;
BEGIN

    SELECT ARRAY_AGG(price)
    INTO short_array
    FROM (SELECT price FROM target_company_stock_prices WHERE company = 'WMT') AS subquery;

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
            SELECT best_index, min_euclidean 
            FROM find_min_pure_euclidean_subarray(long_array, short_array)
        LOOP
            -- 打印結果
            INSERT INTO wmt_euclidean_results (company, best_index, min_dis)
            VALUES (companyname, index+company_start_index-1, min_d);
            RAISE NOTICE 'Company: %, Best Index: %, Min DTW: %, start index: %', companyname, index, min_d, index+company_start_index-1;
        END LOOP;
    END LOOP;
END $$;

UPDATE wmt_euclidean_results
SET start_time = cs.timestamp
FROM company_stock_prices cs
WHERE wmt_euclidean_results.company = cs.company
  AND wmt_euclidean_results.best_index = cs.id;

UPDATE wmt_euclidean_results
SET end_time = cs.timestamp
FROM company_stock_prices cs
WHERE wmt_euclidean_results.company = cs.company
  AND (wmt_euclidean_results.best_index+1258) = cs.id;

SELECT * FROM wmt_euclidean_results;

