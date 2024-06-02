DROP TABLE IF EXISTS appl_euclidean_results;

CREATE TABLE appl_euclidean_results (
    company TEXT,
    min_dis DOUBLE PRECISION
);

DO $$
DECLARE
    companies TEXT[] := ARRAY['WGC', 'QCOM', 'ABT', 'AVGO', 'LMAHD', 'AMGN', 'IOEU4', 'BRK', 'GE', 'DIS', 'jpm us equity', 'WMT', 'NFLX', 'JNJ', 'MA', 'CRM', 'V', 'PFE', 'VZ', 'UNH', 'cvx', 'TSLA', 'PEP', 'LLY', 'WTI', 'INTU', 'ORCL', 'GOOGL', 'AMZN', 'NVDA', 'MSFT', 'BAC', 'LMCAD', 'AMD', 'HD', 'ACN', 'USDCNY', 'XOM', 'CSCO', 'DOLLAR', 'MCD', 'CAT', 'APPL', 'KO', 'PG', 'ADBE', 'GOLD', 'COST', 'TXN'];
    -- 現在先排除掉 corn 跟 那些以月計算的
    -- companies TEXT[] := ARRAY['ABT', 'ACN'];
    companyname TEXT;
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    index INT;
    min_d DOUBLE PRECISION;
BEGIN

    SELECT ARRAY_AGG(price)
    INTO short_array
    FROM (SELECT price FROM target_company_stock_prices WHERE company = 'APPL' ORDER BY timestamp ASC) AS subquery;

    -- 遍歷公司列表
    FOR i IN 1..array_length(companies, 1) LOOP
        companyname := companies[i];
        
        -- 使用 ARRAY_AGG 聚合函數暫存查詢結果
        SELECT ARRAY_AGG(price)
        INTO long_array
        FROM (SELECT price FROM company_stock_prices WHERE company = companyname AND timestamp >= '2017-01-03' AND timestamp < '2022-01-01' ORDER BY timestamp ASC) AS subquery;

        -- 調用 find_min_dtw_subarray 函數並傳入存儲的陣列
        FOR index, min_d IN 
            SELECT best_index, min_euclidean 
            FROM find_min_pure_euclidean_subarray(long_array, short_array)
        LOOP
            -- 打印結果
            INSERT INTO appl_euclidean_results (company, min_dis)
            VALUES (companyname, min_d);
            RAISE NOTICE 'Company: %, Min DTW: %', companyname, min_d;
        END LOOP;
    END LOOP;
END $$;

SELECT * FROM appl_euclidean_results;

