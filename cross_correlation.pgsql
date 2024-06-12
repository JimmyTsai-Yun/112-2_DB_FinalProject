
CREATE TABLE company_stock_prices (
    id SERIAL PRIMARY KEY,
    company TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    price NUMERIC NOT NULL
);

SELECT create_hypertable('company_stock_prices', 'timestamp');
----------------------------------------------
-- function
CREATE OR REPLACE FUNCTION cross_correlation(ts1 DOUBLE PRECISION[], ts2 DOUBLE PRECISION[])
RETURNS TABLE (lag INT, correlation DOUBLE PRECISION) AS $$
DECLARE
    n INT := array_length(ts1, 1);
    m INT := array_length(ts2, 1);
    max_lag INT := LEAST(n, m) - 1;
    lag INT;
    corr DOUBLE PRECISION;
    mean_ts1 DOUBLE PRECISION := 0;
    mean_ts2 DOUBLE PRECISION := 0;
    std_ts1 DOUBLE PRECISION := 0;
    std_ts2 DOUBLE PRECISION := 0;
    cov DOUBLE PRECISION;
    i INT;
BEGIN
    IF n IS NULL OR m IS NULL THEN
        RETURN;
    END IF;

    -- Calculate means
    FOR i IN 1..n LOOP
        mean_ts1 := mean_ts1 + ts1[i];
    END LOOP;
    mean_ts1 := mean_ts1 / n;

    FOR i IN 1..m LOOP
        mean_ts2 := mean_ts2 + ts2[i];
    END LOOP;
    mean_ts2 := mean_ts2 / m;

    -- Calculate standard deviations
    FOR i IN 1..n LOOP
        std_ts1 := std_ts1 + (ts1[i] - mean_ts1) ^ 2;
    END LOOP;
    std_ts1 := sqrt(std_ts1 / n);

    FOR i IN 1..m LOOP
        std_ts2 := std_ts2 + (ts2[i] - mean_ts2) ^ 2;
    END LOOP;
    std_ts2 := sqrt(std_ts2 / m);

    -- Normalize the time series
    FOR i IN 1..n LOOP
        ts1[i] := (ts1[i] - mean_ts1) / std_ts1;
    END LOOP;

    FOR i IN 1..m LOOP
        ts2[i] := (ts2[i] - mean_ts2) / std_ts2;
    END LOOP;

    -- Calculate cross-correlation for each lag
    FOR lag IN -max_lag..max_lag LOOP
        cov := 0;

        IF lag < 0 THEN
            FOR i IN 1..(n + lag) LOOP
                cov := cov + ts1[i] * ts2[i - lag];
            END LOOP;
        ELSE
            FOR i IN (lag + 1)..n LOOP
                cov := cov + ts1[i] * ts2[i - lag];
            END LOOP;
        END IF;

        corr := cov / (n - abs(lag));
        RETURN QUERY SELECT lag, corr;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------
--select
WITH us_ppi_prices AS (
    SELECT ARRAY_AGG(price ORDER BY timestamp) AS prices
    FROM company_stock_prices
    WHERE company = 'US PPI'
),
cross_correlations AS (
    SELECT 
        company,
        lag,
        correlation
    FROM (
        SELECT 
            company,
            lag,
            correlation,
            ROW_NUMBER() OVER (PARTITION BY company ORDER BY correlation DESC) AS row_num
        FROM (
            SELECT 
                c2.company,
                c.lag,
                c.correlation
            FROM (
                SELECT 
                    company,
                    ARRAY_AGG(price ORDER BY timestamp) AS prices
                FROM company_stock_prices
                WHERE company != 'US PPI'
                GROUP BY company
            ) AS c2,
            LATERAL cross_correlation(
                (SELECT prices FROM us_ppi_prices),
                c2.prices
            ) AS c
        ) AS subquery
        WHERE lag BETWEEN -100 AND 100
    ) AS ranked
    WHERE row_num = 1
)
SELECT company, lag, correlation
FROM cross_correlations
ORDER BY correlation DESC;