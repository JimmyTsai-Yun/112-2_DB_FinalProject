-- ALTER TABLE company_dtw_results
-- ADD COLUMN start_time TIMESTAMP,
-- ADD COLUMN end_time TIMESTAMP;

-- UPDATE company_dtw_results
-- SET start_time = cs.timestamp
-- FROM company_stock_prices cs
-- WHERE company_dtw_results.company = cs.company
--   AND company_dtw_results.best_index = cs.id;

-- UPDATE company_dtw_results
-- SET end_time = cs.timestamp
-- FROM company_stock_prices cs
-- WHERE company_dtw_results.company = cs.company
--   AND (company_dtw_results.best_index+41) = cs.id;

SELECT * FROM company_dtw_results;
-- DO $$
-- DECLARE
--     long_array DOUBLE PRECISION[];
--     short_array DOUBLE PRECISION[];
--     index INT;
--     min_d DOUBLE PRECISION;
-- BEGIN

--     SELECT ARRAY_AGG(price)
--     INTO short_array
--     FROM (SELECT price FROM target_company_stock_prices WHERE company = 'GOLD_WAR') AS subquery;

--     SELECT ARRAY_AGG(price)
--     INTO long_array
--     FROM (SELECT price FROM company_stock_prices WHERE company = 'AVGO' AND id BETWEEN 17831 AND 17872) AS subquery;

--     For index, min_d IN 
--         SELECT best_index, min_dtw 
--         FROM find_min_dtw_subarray(long_array, short_array)
--     LOOP
--         RAISE NOTICE 'Best Index: %, Min DTW: %', index, min_d;
--     END LOOP;
-- END $$;

-- SELECT * 
-- FROM company_stock_prices
-- WHERE company = 'AVGO' 
-- limit 4;