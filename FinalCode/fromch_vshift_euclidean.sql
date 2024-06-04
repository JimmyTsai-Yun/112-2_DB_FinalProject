DROP TABLE IF EXISTS fromch_vsift_results;

CREATE TABLE fromch_vsift_results (
    company TEXT,
    min_dis DOUBLE PRECISION
);

DO $$
DECLARE
    companies TEXT[] := ARRAY['WGC', 'QCOM', 'ABT', 'AVGO', 'LMAHD', 'AMGN', 'IOEU4', 'BRK', 'GE', 'DIS', 'jpm us equity', 'WMT', 'NFLX', 'JNJ', 'MA', 'CRM', 'V', 'PFE', 'VZ', 'UNH', 'cvx', 'TSLA', 'PEP', 'LLY', 'WTI', 'INTU', 'ORCL', 'GOOGL', 'AMZN', 'NVDA', 'MSFT', 'BAC', 'LMCAD', 'AMD', 'HD', 'ACN', 'USDCNY', 'XOM', 'CSCO', 'DOLLAR', 'MCD', 'CAT', 'APPL', 'KO', 'PG', 'ADBE', 'GOLD', 'COST', 'TXN'];
    -- companies TEXT[] := ARRAY['ABT', 'ACN'];
    companyname TEXT;
    long_array DOUBLE PRECISION[];
    short_array DOUBLE PRECISION[];
    index INT;
    min_d DOUBLE PRECISION;
BEGIN

    SELECT ARRAY_AGG(price)
    INTO short_array
    FROM (SELECT price FROM target_company_stock_prices WHERE company = 'IMPORTCN' ORDER BY timestamp ASC) AS subquery;

    FOR i IN 1..array_length(companies, 1) LOOP
        companyname := companies[i];

        SELECT ARRAY_AGG(price)
        INTO long_array
        FROM (SELECT price FROM company_stock_prices WHERE company = companyname AND timestamp >= '2017-01-01' AND timestamp < '2022-01-02' ORDER BY timestamp ASC) AS subquery;

        FOR index, min_d IN 
            SELECT best_index, min_vshift_euclidean 
            FROM find_min_vshift_euclidean_subarray(long_array, short_array)
        LOOP
            INSERT INTO fromch_vsift_results (company, min_dis)
            VALUES (companyname, min_d);
            RAISE NOTICE 'Company: %, Min DTW: %', companyname, min_d;
        END LOOP;
    END LOOP;
END $$;

SELECT * FROM fromch_vsift_results;
