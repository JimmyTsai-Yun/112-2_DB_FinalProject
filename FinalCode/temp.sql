-- SELECT company, count(*)
-- FROM company_stock_prices
-- WHERE timestamp >= '2017-01-01' AND timestamp < '2022-01-02'
-- GROUP BY company;

-- SELECT count(*)
-- FROM target_company_stock_prices
-- WHERE company = 'GOLD_WAR';

-- SELECT * FROM company_stock_prices
-- WHERE company = 'GOLD';

-- SELECT id, time_bucket('6 days', timestamp) as period
--         INTO company_start_index
--         FROM company_stock_prices
--         WHERE company = 'GOLD'
--         ORDER BY time_bucket('6 days', timestamp), timestamp ASC
--         LIMIT 1;

SELECT * FROM calculate_mixed_with_timerange('NVDA', ARRAY['APPL'], TRUE, '2012-01-01', '2012-01-10', ARRAY[1,2,3]);
-- SELECT * FROM calculate_pure_euclidean_results('NVDA', ARRAY['NVDA'], TRUE);
-- SELECT * FROM calculate_vshift_results('NVDA', ARRAY['NVDA'], TRUE);
-- SELECT count(*) FROM company_stock_prices where company = 'APPL';