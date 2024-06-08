-- DROP TABLE IF EXISTS fromch_vsift_results;

-- CREATE TABLE fromch_vsift_results (
--     company TEXT,
--     min_dis DOUBLE PRECISION
-- );

-- DO $$
-- DECLARE
--     companies TEXT[] := ARRAY['WGC', 'QCOM', 'ABT', 'AVGO', 'LMAHD', 'AMGN', 'IOEU4', 'BRK', 'GE', 'DIS', 'jpm us equity', 'WMT', 'NFLX', 'JNJ', 'MA', 'CRM', 'V', 'PFE', 'VZ', 'UNH', 'cvx', 'TSLA', 'PEP', 'LLY', 'WTI', 'INTU', 'ORCL', 'GOOGL', 'AMZN', 'NVDA', 'MSFT', 'BAC', 'LMCAD', 'AMD', 'HD', 'ACN', 'USDCNY', 'XOM', 'CSCO', 'DOLLAR', 'MCD', 'CAT', 'APPL', 'KO', 'PG', 'ADBE', 'GOLD', 'COST', 'TXN'];
--     -- companies TEXT[] := ARRAY['ABT', 'ACN'];
--     companyname TEXT;
--     long_array DOUBLE PRECISION[];
--     short_array DOUBLE PRECISION[];
--     index INT;
--     min_d DOUBLE PRECISION;
-- BEGIN

--     SELECT ARRAY_AGG(price)
--     INTO short_array
--     FROM (SELECT price FROM target_company_stock_prices WHERE company = 'IMPORTCN' ORDER BY timestamp ASC) AS subquery;

--     FOR i IN 1..array_length(companies, 1) LOOP
--         companyname := companies[i];

--         SELECT ARRAY_AGG(price)
--         INTO long_array
--         FROM (SELECT price FROM company_stock_prices WHERE company = companyname AND timestamp >= '2017-01-01' AND timestamp < '2022-01-02' ORDER BY timestamp ASC) AS subquery;

--         FOR index, min_d IN 
--             SELECT best_index, min_vshift_euclidean 
--             FROM find_min_vshift_euclidean_subarray(long_array, short_array)
--         LOOP
--             INSERT INTO fromch_vsift_results (company, min_dis)
--             VALUES (companyname, min_d);
--             RAISE NOTICE 'Company: %, Min DTW: %', companyname, min_d;
--         END LOOP;
--     END LOOP;
-- END $$;

-- SELECT * FROM fromch_vsift_results;

DROP FUNCTION IF EXISTS calculate_vshift_results(TEXT, TEXT[], BOOLEAN);

-- CREATE OR REPLACE FUNCTION calculate_vshift_results(input_company TEXT)
-- RETURNS TABLE(companyName TEXT, min_dis DOUBLE PRECISION) AS $$
-- DECLARE
--     companies TEXT[];
--     long_array DOUBLE PRECISION[];
--     short_array DOUBLE PRECISION[];
-- BEGIN
--     -- 從指定的表中動態提取所有公司名稱到數組
--     SELECT array_agg(DISTINCT company) INTO companies FROM company_stock_prices;

--     -- 從input_company抓取short_array的數據
--     SELECT ARRAY_AGG(price) INTO short_array FROM (SELECT price FROM target_company_stock_prices WHERE company = input_company ORDER BY timestamp ASC) AS subquery;

--     -- 對每一間公司進行處理
--     FOR i IN 1..array_length(companies, 1) LOOP
--         SELECT ARRAY_AGG(price) INTO long_array FROM (SELECT price FROM company_stock_prices WHERE company = companies[i] AND timestamp >= '2017-01-01' AND timestamp < '2022-01-02' ORDER BY timestamp ASC) AS subquery;
--         RETURN QUERY SELECT companies[i], min_vshift_euclidean FROM find_min_vshift_euclidean_subarray(long_array, short_array);
--     END LOOP;
-- END;
-- $$ LANGUAGE plpgsql;

-- SELECT * FROM calculate_vshift_results('IMPORTCN');

-- CREATE OR REPLACE FUNCTION calculate_vshift_results(input_company TEXT, use_specific_range BOOLEAN)
-- RETURNS TABLE(companyName TEXT, min_dis DOUBLE PRECISION) AS $$
-- DECLARE
--     companies TEXT[];
--     long_array DOUBLE PRECISION[];
--     short_array DOUBLE PRECISION[];
--     start_date TIMESTAMP;
--     end_date TIMESTAMP;
-- BEGIN
--     -- 從指定的表中動態提取所有公司名稱到數組
--     SELECT array_agg(DISTINCT company) INTO companies FROM company_stock_prices;

--     -- 從input_company抓取short_array的數據
--     SELECT ARRAY_AGG(price) INTO short_array FROM (
--         SELECT price FROM target_company_stock_prices WHERE company = input_company ORDER BY timestamp ASC
--     ) AS subquery;

--     IF use_specific_range THEN
--         SELECT MIN(timestamp), MAX(timestamp) INTO start_date, end_date FROM target_company_stock_prices WHERE company = input_company;
--     END IF;

--     -- -- print start_date, end_date;
--     -- RAISE NOTICE 'Start Date: %, End Date: %', start_date, end_date;

--     -- 對每一間公司進行處理
--     FOR i IN 1..array_length(companies, 1) LOOP
--         -- 構造查詢字符串
--         IF use_specific_range THEN
--             SELECT ARRAY_AGG(price) INTO long_array FROM (
--             SELECT price FROM company_stock_prices WHERE company = companies[i] AND timestamp >= start_date AND timestamp <= end_date ORDER BY timestamp ASC
--         ) AS subquery;
--         ELSE
--             SELECT ARRAY_AGG(price) INTO long_array FROM (
--             SELECT price FROM company_stock_prices WHERE company = companies[i] ORDER BY timestamp ASC
--         ) AS subquery;
--         END IF;

--         -- 假設 find_min_vshift_euclidean_subarray 是一個已定義的函數來計算距離
--         RETURN QUERY SELECT companies[i], min_vshift_euclidean FROM find_min_vshift_euclidean_subarray(long_array, short_array);
--     END LOOP;
-- END;
-- $$ LANGUAGE plpgsql;


-- -- 使用特定時間範圍
-- SELECT * FROM calculate_vshift_results('IMPORTCN', TRUE);

CREATE OR REPLACE FUNCTION calculate_vshift_results(input_company TEXT, compare_company_list TEXT[], use_specific_range BOOLEAN)
RETURNS TABLE(companyName TEXT, min_dis DOUBLE PRECISION, start_time timestamp, end_time timestamp) AS $$
DECLARE
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    start_date TIMESTAMP;
    end_date TIMESTAMP;
    min_vshift_euclidean_record DOUBLE PRECISION;
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

    -- -- print start_date, end_date;
    -- RAISE NOTICE 'Start Date: %, End Date: %', start_date, end_date;

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

        SELECT min_vshift_euclidean, best_index INTO min_vshift_euclidean_record, best_index_record FROM find_min_vshift_euclidean_subarray(long_array, short_array);
        SELECT timestamp INTO start_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex - 1);
        SELECT timestamp INTO end_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex + array_length(short_array, 1) - 2);
        -- 返回結果

        -- 假設 find_min_vshift_euclidean_subarray 是一個已定義的函數來計算距離
        RETURN QUERY SELECT compare_company_list[i], min_vshift_euclidean_record, start_time, end_time;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- 使用特定時間範圍
-- SELECT * FROM calculate_vshift_results('IMPORTCN', ARRAY['ABT'], FALSE);

DROP FUNCTION IF EXISTS calculate_vshift_with_timerange(TEXT, TEXT[], BOOLEAN, timestamp, timestamp);

CREATE OR REPLACE FUNCTION calculate_vshift_with_timerange(input_company TEXT, compare_company_list TEXT[], use_specific_range BOOLEAN, start_day timestamp, end_day timestamp)
RETURNS TABLE(companyName TEXT, min_dis DOUBLE PRECISION, start_time timestamp, end_time timestamp) AS $$
DECLARE
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    min_vshift_euclidean_record DOUBLE PRECISION;
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

        SELECT min_vshift_euclidean, best_index INTO min_vshift_euclidean_record, best_index_record FROM find_min_vshift_euclidean_subarray(long_array, short_array);
        SELECT timestamp INTO start_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex - 1);
        SELECT timestamp INTO end_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex + array_length(short_array, 1) - 2);
        -- 返回結果

        -- 假設 find_min_vshift_euclidean_subarray 是一個已定義的函數來計算距離
        RETURN QUERY SELECT compare_company_list[i], min_vshift_euclidean_record, start_time, end_time;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 使用特定時間範圍
SELECT * FROM calculate_vshift_with_timerange('IMPORTCN', ARRAY['ABT'], TRUE, '2017-01-01', '2022-01-01');