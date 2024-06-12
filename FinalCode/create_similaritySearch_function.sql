-- pure euclidean 
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
        SELECT price FROM company_stock_prices WHERE company = input_company ORDER BY timestamp ASC
    ) AS subquery;

    IF use_specific_range THEN
        SELECT MIN(timestamp), MAX(timestamp) INTO start_date, end_date FROM company_stock_prices WHERE company = input_company;
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
        SELECT price FROM company_stock_prices WHERE company = input_company AND timestamp >= start_day AND timestamp <= end_day ORDER BY timestamp ASC
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



-- vshift euclidean
DROP FUNCTION IF EXISTS calculate_vshift_results(TEXT, TEXT[], BOOLEAN);
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
        SELECT price FROM company_stock_prices WHERE company = input_company ORDER BY timestamp ASC
    ) AS subquery;

    IF use_specific_range THEN
        SELECT MIN(timestamp), MAX(timestamp) INTO start_date, end_date FROM company_stock_prices WHERE company = input_company;
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

        SELECT min_vshift_euclidean, best_index INTO min_vshift_euclidean_record, best_index_record FROM find_min_vshift_euclidean_subarray(long_array, short_array);
        SELECT timestamp INTO start_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex - 1);
        SELECT timestamp INTO end_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex + array_length(short_array, 1) - 2);
        -- 返回結果

        -- 假設 find_min_vshift_euclidean_subarray 是一個已定義的函數來計算距離
        RETURN QUERY SELECT compare_company_list[i], min_vshift_euclidean_record, start_time, end_time;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

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
        SELECT price FROM company_stock_prices WHERE company = input_company AND timestamp >= start_day AND timestamp <= end_day ORDER BY timestamp ASC
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

-- dtw
DROP FUNCTION IF EXISTS calculate_dtw_results(TEXT, TEXT[], BOOLEAN);

CREATE OR REPLACE FUNCTION calculate_dtw_results(input_company TEXT, compare_company_list TEXT[], use_specific_range BOOLEAN)
RETURNS TABLE(companyName TEXT, min_dis DOUBLE PRECISION, start_time timestamp, end_time timestamp) AS $$
DECLARE
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    start_date TIMESTAMP;
    end_date TIMESTAMP;
    min_dtw_record DOUBLE PRECISION;
    companyfirstindex INT;
    best_index_record INT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN

    -- 從input_company抓取short_array的數據
    SELECT ARRAY_AGG(price) INTO short_array FROM (
        SELECT price FROM company_stock_prices WHERE company = input_company ORDER BY timestamp ASC
    ) AS subquery;

    IF use_specific_range THEN
        SELECT MIN(timestamp), MAX(timestamp) INTO start_date, end_date FROM company_stock_prices WHERE company = input_company;
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

        SELECT min_dtw, best_index INTO min_dtw_record, best_index_record FROM find_min_dtw_subarray(long_array, short_array);
        SELECT timestamp INTO start_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex - 1);
        SELECT timestamp INTO end_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex + array_length(short_array, 1) - 2);
        -- 返回結果

        -- 假設 find_min_vshift_euclidean_subarray 是一個已定義的函數來計算距離
        RETURN QUERY SELECT compare_company_list[i], min_dtw_record, start_time, end_time;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS calculate_dtw_with_timerange(TEXT, TEXT[], BOOLEAN, TIMESTAMP, TIMESTAMP);

CREATE OR REPLACE FUNCTION calculate_dtw_with_timerange(input_company TEXT, compare_company_list TEXT[], use_specific_range BOOLEAN, start_day timestamp, end_day timestamp)
RETURNS TABLE(companyName TEXT, min_dis DOUBLE PRECISION, start_time timestamp, end_time timestamp) AS $$
DECLARE
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    min_dtw_record DOUBLE PRECISION;
    companyfirstindex INT;
    best_index_record INT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN

    -- 從input_company抓取short_array的數據
    SELECT ARRAY_AGG(price) INTO short_array FROM (
        SELECT price FROM company_stock_prices WHERE company = input_company AND timestamp >= start_day AND timestamp <= end_day ORDER BY timestamp ASC
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
        SELECT min_dtw, best_index INTO min_dtw_record, best_index_record FROM find_min_dtw_subarray(long_array, short_array);
        SELECT timestamp INTO start_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex - 1);
        SELECT timestamp INTO end_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex + array_length(short_array, 1) - 2);
        -- 返回結果
        RETURN QUERY SELECT compare_company_list[i], min_dtw_record, start_time, end_time;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


--- mixed distance
DROP FUNCTION IF EXISTS calculate_mixed_results(TEXT, TEXT[], BOOLEAN, INT[], weight INT[]);
CREATE OR REPLACE FUNCTION calculate_mixed_results(input_company TEXT, compare_company_list TEXT[], use_specific_range BOOLEAN, weight INT[])
RETURNS TABLE(companyName TEXT, min_dis DOUBLE PRECISION, start_time timestamp, end_time timestamp) AS $$
DECLARE
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    start_date TIMESTAMP;
    end_date TIMESTAMP;
    min_mixed_record DOUBLE PRECISION;
    companyfirstindex INT;
    best_index_record INT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
        -- 從input_company抓取short_array的數據
        SELECT ARRAY_AGG(price) INTO short_array FROM (
            SELECT price FROM company_stock_prices WHERE company = input_company ORDER BY timestamp ASC
        ) AS subquery;
    
        IF use_specific_range THEN
            SELECT MIN(timestamp), MAX(timestamp) INTO start_date, end_date FROM company_stock_prices WHERE company = input_company;
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
    
            SELECT min_mixed, best_index INTO min_mixed_record, best_index_record FROM find_min_mixed_subarray(long_array, short_array, weight);
            SELECT timestamp INTO start_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex - 1);
            SELECT timestamp INTO end_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex + array_length(short_array, 1) - 2);
            -- 返回結果
    
            -- 假設 find_min_vshift_euclidean_subarray 是一個已定義的函數來計算距離
            RETURN QUERY SELECT compare_company_list[i], min_mixed_record, start_time, end_time;
        END LOOP;
    END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS calculate_mixed_with_timerange(TEXT, TEXT[], BOOLEAN, INT[], timestamp, timestamp, weight INT[]);

CREATE OR REPLACE FUNCTION calculate_mixed_with_timerange(input_company TEXT, compare_company_list TEXT[], use_specific_range BOOLEAN, start_day timestamp, end_day timestamp, weight INT[])
RETURNS TABLE(companyName TEXT, min_dis DOUBLE PRECISION, start_time timestamp, end_time timestamp) AS $$
DECLARE
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    min_mixed_record DOUBLE PRECISION;
    companyfirstindex INT;
    best_index_record INT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN   
        -- 從input_company抓取short_array的數據
        SELECT ARRAY_AGG(price) INTO short_array FROM (
            SELECT price FROM company_stock_prices WHERE company = input_company AND timestamp >= start_day AND timestamp <= end_day ORDER BY timestamp ASC
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
    
            SELECT min_mixed, best_index INTO min_mixed_record, best_index_record FROM find_min_mixed_subarray(long_array, short_array, weight);
            SELECT timestamp INTO start_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex - 1);
            SELECT timestamp INTO end_time FROM company_stock_prices WHERE company = compare_company_list[i] AND id = (best_index_record + companyfirstindex + array_length(short_array, 1) - 2);
            -- 返回結果
    
            -- 假設 find_min_vshift_euclidean_subarray 是一個已定義的函數來計算距離
            RETURN QUERY SELECT compare_company_list[i], min_mixed_record, start_time, end_time;
        END LOOP;
    END;
$$ LANGUAGE plpgsql;