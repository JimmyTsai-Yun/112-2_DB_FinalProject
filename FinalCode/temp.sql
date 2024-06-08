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

SELECT * FROM target_company_stock_prices where company = 'IMPORTCN';