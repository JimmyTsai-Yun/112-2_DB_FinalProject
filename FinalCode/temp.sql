-- SELECT * FROM calculate_mixed_with_timerange('NVDA', ARRAY['APPL', 'ABT'], TRUE, '2012-01-01', '2012-01-10', ARRAY[1,2,3]);
SELECT * FROM calculate_dtw_with_timerange('NVDA', ARRAY['APPL', 'ABT', 'BAC'], TRUE, '2012-01-01', '2012-01-10');
-- SELECT * FROM calculate_pure_euclidean_results('NVDA', ARRAY['NVDA'], TRUE);
-- SELECT * FROM calculate_vshift_results('NVDA', ARRAY['NVDA'], TRUE);
-- SELECT count(*) FROM company_stock_prices where company = 'APPL';