-- DROP TABLE IF EXISTS appl_euclidean_results;

-- CREATE TABLE appl_euclidean_results (
--     company TEXT,
--     min_dis DOUBLE PRECISION
-- );

-- DO $$
-- DECLARE
--     companies TEXT[] := ARRAY['WGC', 'QCOM', 'ABT', 'AVGO', 'LMAHD', 'AMGN', 'IOEU4', 'BRK', 'GE', 'DIS', 'jpm us equity', 'WMT', 'NFLX', 'JNJ', 'MA', 'CRM', 'V', 'PFE', 'VZ', 'UNH', 'cvx', 'TSLA', 'PEP', 'LLY', 'WTI', 'INTU', 'ORCL', 'GOOGL', 'AMZN', 'NVDA', 'MSFT', 'BAC', 'LMCAD', 'AMD', 'HD', 'ACN', 'USDCNY', 'XOM', 'CSCO', 'DOLLAR', 'MCD', 'CAT', 'APPL', 'KO', 'PG', 'ADBE', 'GOLD', 'COST', 'TXN'];
--     -- 現在先排除掉 corn 跟 那些以月計算的
--     -- companies TEXT[] := ARRAY['ABT', 'ACN'];
--     companyname TEXT;
--     long_array DOUBLE PRECISION[];
--     short_array DOUBLE PRECISION[];
--     index INT;
--     min_d DOUBLE PRECISION;
-- BEGIN

--     SELECT ARRAY_AGG(price)
--     INTO short_array
--     FROM (SELECT price FROM target_company_stock_prices WHERE company = 'APPL' ORDER BY timestamp ASC) AS subquery;

--     -- 遍歷公司列表
--     FOR i IN 1..array_length(companies, 1) LOOP
--         companyname := companies[i];
        
--         -- 使用 ARRAY_AGG 聚合函數暫存查詢結果
--         SELECT ARRAY_AGG(price)
--         INTO long_array
--         FROM (SELECT price FROM company_stock_prices WHERE company = companyname AND timestamp >= '2017-01-03' AND timestamp < '2022-01-01' ORDER BY timestamp ASC) AS subquery;

--         -- 調用 find_min_dtw_subarray 函數並傳入存儲的陣列
--         FOR index, min_d IN 
--             SELECT best_index, min_euclidean 
--             FROM find_min_pure_euclidean_subarray(long_array, short_array)
--         LOOP
--             -- 打印結果
--             INSERT INTO appl_euclidean_results (company, min_dis)
--             VALUES (companyname, min_d);
--             RAISE NOTICE 'Company: %, Min DTW: %', companyname, min_d;
--         END LOOP;
--     END LOOP;
-- END $$;

-- SELECT * FROM appl_euclidean_results;

DROP FUNCTION IF EXISTS calculate_pure_euclidean_results(TEXT, TEXT[], BOOLEAN);

CREATE OR REPLACE FUNCTION calculate_pure_euclidean_results(input_company TEXT, compare_company_list TEXT[], use_specific_range BOOLEAN)
RETURNS TABLE(companyName TEXT, min_dis DOUBLE PRECISION, start_time timestamp, end_time timestamp) AS $$
DECLARE
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    start_date TIMESTAMP;
    end_date TIMESTAMP;
    min_euclidean_record DOUBLE PRECISION;
    companyfirstindex INT;
    best_index_record INT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN

    -- 從input_company抓取short_array的數據
    SELECT ARRAY_AGG(price) INTO short_array FROM (
        SELECT price FROM target_company_stock_prices WHERE company = input_company ORDER BY timestamp ASC
    ) AS subquery;

    IF use_specific_range THEN
        SELECT MIN(timestamp), MAX(timestamp) INTO start_date, end_date FROM target_company_stock_prices WHERE company = input_company;
    END IF;

    -- 對每一間公司進行處理
    FOR i IN 1..array_length(compare_company_list, 1) LOOP
        -- 構造查詢字符串
        IF use_specific_range THEN
            SELECT ARRAY_AGG(price) INTO long_array FROM (
            SELECT price FROM company_stock_prices WHERE company = compare_company_list[i] AND timestamp >= start_date AND timestamp <= end_date ORDER BY timestamp ASC
        ) AS subquery;
            SELECT id INTO companyfirstindex FROM company_stock_prices WHERE company = compare_company_list[i] AND timestamp = start_date;
        ELSE
            SELECT ARRAY_AGG(price) INTO long_array FROM (
            SELECT price FROM company_stock_prices WHERE company = compare_company_list[i] ORDER BY timestamp ASC
        ) AS subquery;
            SELECT id INTO companyfirstindex FROM company_stock_prices WHERE company = compare_company_list[i] ORDER BY timestamp ASC LIMIT 1;
        END IF;

        -- 假設 find_min_vshift_euclidean_subarray 是一個已定義的函數來計算距離
        SELECT min_euclidean, best_index INTO min_euclidean_record, best_index_record FROM find_min_pure_euclidean_subarray(long_array, short_array);
        SELECT timestamp INTO start_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex - 1);
        SELECT timestamp INTO end_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex + array_length(short_array, 1) - 2);
        -- 返回結果
        RETURN QUERY SELECT compare_company_list[i], min_euclidean_record, start_time, end_time;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- 使用特定時間範圍
SELECT * FROM calculate_pure_euclidean_results('APPL', ARRAY['AVGO'], FALSE);


DROP FUNCTION IF EXISTS calculate_pure_euclidean_with_timerange(TEXT, TEXT[], BOOLEAN, timestamp, timestamp);

CREATE OR REPLACE FUNCTION calculate_pure_euclidean_with_timerange(input_company TEXT, compare_company_list TEXT[], use_specific_range BOOLEAN, start_day timestamp, end_day timestamp)
RETURNS TABLE(companyName TEXT, min_dis DOUBLE PRECISION, start_time timestamp, end_time timestamp) AS $$
DECLARE
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    min_euclidean_record DOUBLE PRECISION;
    companyfirstindex INT;
    best_index_record INT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN

    -- 從input_company抓取short_array的數據
    SELECT ARRAY_AGG(price) INTO short_array FROM (
        SELECT price FROM target_company_stock_prices WHERE company = input_company AND timestamp >= start_day AND timestamp <= end_day ORDER BY timestamp ASC
    ) AS subquery;

    -- 對每一間公司進行處理
    FOR i IN 1..array_length(compare_company_list, 1) LOOP
        -- 構造查詢字符串
        IF use_specific_range THEN
            SELECT ARRAY_AGG(price) INTO long_array FROM (
            SELECT price FROM company_stock_prices WHERE company = compare_company_list[i] AND timestamp >= start_day AND timestamp <= end_day ORDER BY timestamp ASC
        ) AS subquery;
            SELECT id INTO companyfirstindex FROM company_stock_prices WHERE company = compare_company_list[i] AND timestamp = start_day;
        ELSE
            SELECT ARRAY_AGG(price) INTO long_array FROM (
            SELECT price FROM company_stock_prices WHERE company = compare_company_list[i] ORDER BY timestamp ASC
        ) AS subquery;
            SELECT id INTO companyfirstindex FROM company_stock_prices WHERE company = compare_company_list[i] ORDER BY timestamp ASC LIMIT 1;
        END IF;

        -- 假設 find_min_vshift_euclidean_subarray 是一個已定義的函數來計算距離
        SELECT min_euclidean, best_index INTO min_euclidean_record, best_index_record FROM find_min_pure_euclidean_subarray(long_array, short_array);
        SELECT timestamp INTO start_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex - 1);
        SELECT timestamp INTO end_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex + array_length(short_array, 1) - 2);
        -- 返回結果
        RETURN QUERY SELECT compare_company_list[i], min_euclidean_record, start_time, end_time;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM calculate_pure_euclidean_with_timerange('APPL', ARRAY['AVGO'], TRUE, '2017-01-03', '2022-12-31');