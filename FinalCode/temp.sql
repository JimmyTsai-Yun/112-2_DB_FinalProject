-- SELECT company, count(*)
-- FROM company_stock_prices
-- WHERE timestamp >= '2017-01-01' AND timestamp < '2022-01-02'
-- GROUP BY company;

-- SELECT count(*)
-- FROM target_company_stock_prices
-- WHERE company = 'GOLD_WAR';

SELECT * FROM company_stock_prices
WHERE company = 'GOLD';
