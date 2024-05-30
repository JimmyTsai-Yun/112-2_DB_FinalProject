-- password for postgres: 123412341234

-- CREATE DTW distance for estimating the distance between two time series
CREATE OR REPLACE FUNCTION dtw_distance(ts1 DOUBLE PRECISION[], ts2 DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION AS $$
DECLARE
    n INT;
    m INT;
    i INT;
    j INT;
    cost DOUBLE PRECISION;
    total_cost DOUBLE PRECISION;
    dtw_table DOUBLE PRECISION[][];

BEGIN
    n := array_length(ts1, 1);
    m := array_length(ts2, 1);

    -- 初始化 DTW 矩陣
    IF n IS NULL OR m IS NULL THEN
        RETURN NULL;
    END IF;

    -- 手動初始化矩陣大小及設置初始值為無限大
    dtw_table := ARRAY(SELECT ARRAY(SELECT 'infinity'::DOUBLE PRECISION FROM generate_series(1, m+1)) FROM generate_series(1, n+1));
    dtw_table[1][1] := 0;

    -- 填充 DTW 矩陣
    FOR i IN 2..n+1 LOOP
        FOR j IN 2..m+1 LOOP
            cost := abs(ts1[i-1] - ts2[j-1]);
            dtw_table[i][j] := cost + LEAST(dtw_table[i-1][j],    
                                            dtw_table[i][j-1],    
                                            dtw_table[i-1][j-1]); 
        END LOOP;
    END LOOP;

    -- 追踪最佳路徑並計算總成本
    total_cost := 0;
    i := n+1;
    j := m+1;

    WHILE i > 1 OR j > 1 LOOP
        total_cost := total_cost + dtw_table[i][j];

        IF i = 1 THEN
            j := j - 1;
        ELSIF j = 1 THEN
            i := i - 1;
        ELSE
            IF dtw_table[i-1][j] <= dtw_table[i][j-1] AND dtw_table[i-1][j] <= dtw_table[i-1][j-1] THEN
                i := i - 1;
            ELSIF dtw_table[i][j-1] <= dtw_table[i-1][j] AND dtw_table[i][j-1] <= dtw_table[i-1][j-1] THEN
                j := j - 1;
            ELSE
                i := i - 1;
                j := j - 1;
            END IF;
        END IF;
    END LOOP;

    -- 返回總成本
    RETURN total_cost;
END;
$$ LANGUAGE plpgsql;


-- Test the DTW distance function
SELECT dtw_distance(ARRAY[1,3,4], ARRAY[7,9]); -- 31
SELECT dtw_distance(ARRAY[1,6,2,3,0,9,4,3,6,3], ARRAY[1,3,4,9,8,2,1,5,7,3]); -- 104

-- CREATE Euclidean distance for estimating the distance between two time series
CREATE OR REPLACE FUNCTION euclidean_distance(ts1 DOUBLE PRECISION[], ts2 DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION AS $$
DECLARE
    n INT;
    m INT;
    i INT;
    distance DOUBLE PRECISION;

BEGIN
    n := array_length(ts1, 1);
    m := array_length(ts2, 1);

    IF n IS NULL OR m IS NULL THEN
        RETURN NULL;
    END IF;

    IF n <> m THEN
        RETURN NULL;
    END IF;

    distance := 0;

    FOR i IN 1..n LOOP
        distance := distance + (ts1[i] - ts2[i])^2;
    END LOOP;

    RETURN sqrt(distance);
END;
$$ LANGUAGE plpgsql;

-- CREATE  v-shift similar Euclidean distance for estimating the distance between two time series
CREATE OR REPLACE FUNCTION vshift_euclidean_distance(ts1 DOUBLE PRECISION[], ts2 DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION AS $$
DECLARE
    n INT;
    m INT;
    i INT;
    ts1_mean DOUBLE PRECISION;
    ts2_mean DOUBLE PRECISION;
    distance DOUBLE PRECISION;

BEGIN
    n := array_length(ts1, 1);
    m := array_length(ts2, 1);

    IF n IS NULL OR m IS NULL THEN
        RETURN NULL;
    END IF;

    IF n <> m THEN
        RETURN NULL;
    END IF;

    ts1_mean := 0;
    ts2_mean := 0;

    FOR i IN 1..n LOOP
        ts1_mean := ts1_mean + ts1[i];
        ts2_mean := ts2_mean + ts2[i];
    END LOOP;

    ts1_mean := ts1_mean / n;
    ts2_mean := ts2_mean / m;

    distance := 0;
    
    FOR i IN 1..n LOOP
        distance := distance + (ts1[i] - ts1_mean - ts2[i] + ts2_mean)^2;
    END LOOP;
    RETURN sqrt(distance);
END;
$$ LANGUAGE plpgsql;



-- define find_min_dtw_subarray function if it exists
DROP FUNCTION IF EXISTS find_min_dtw_subarray(long_array DOUBLE PRECISION[], short_array DOUBLE PRECISION[]);

CREATE OR REPLACE FUNCTION find_min_dtw_subarray(long_array DOUBLE PRECISION[], short_array DOUBLE PRECISION[])
-- RETURNS DOUBLE PRECISION[] AS $$
RETURNS TABLE (best_index INT, min_dtw DOUBLE PRECISION) AS $$
DECLARE
    subarray_length INT;
    min_dtw DOUBLE PRECISION := 'infinity';
    current_dtw DOUBLE PRECISION;
    best_subarray DOUBLE PRECISION[];
    best_index INT := -1;
    i INT;

BEGIN
    subarray_length := array_length(short_array, 1);

    -- 遍歷 long_array 並計算每個子陣列的 DTW 距離
    FOR i IN 1..array_length(long_array, 1) - subarray_length + 1 LOOP
        -- 獲取子陣列
        current_dtw := dtw_distance(long_array[i:i + subarray_length - 1], short_array);

        -- 檢查是否找到更小的 DTW 距離
        IF current_dtw < min_dtw THEN
            min_dtw := current_dtw;
            best_index := i;
            best_subarray := long_array[i:i + subarray_length - 1];
        END IF;
    END LOOP;

    -- 返回具有最小 DTW 距離的子陣列
    RETURN QUERY SELECT best_index, min_dtw;
END;
$$ LANGUAGE plpgsql;



-- define find_min_pure_euclidean_subarray function
DROP FUNCTION IF EXISTS find_min_pure_euclidean_subarray(long_array DOUBLE PRECISION[], short_array DOUBLE PRECISION[]);

CREATE OR REPLACE FUNCTION find_min_pure_euclidean_subarray(long_array DOUBLE PRECISION[], short_array DOUBLE PRECISION[])
RETURNS TABLE (best_index INT, min_euclidean DOUBLE PRECISION) AS $$
DECLARE
    subarray_length INT;
    min_euclidean DOUBLE PRECISION := 'infinity';
    current_euclidean DOUBLE PRECISION;
    best_subarray DOUBLE PRECISION[];
    best_index INT := -1;
    i INT;

BEGIN
    subarray_length := array_length(short_array, 1);

    -- 遍歷 long_array 並計算每個子陣列的歐幾里得距離
    FOR i IN 1..array_length(long_array, 1) - subarray_length + 1 LOOP
        -- 獲取子陣列
        current_euclidean := euclidean_distance(long_array[i:i + subarray_length - 1], short_array);

        -- 檢查是否找到更小的歐幾里得距離
        IF current_euclidean < min_euclidean THEN
            min_euclidean := current_euclidean;
            best_index := i;
            best_subarray := long_array[i:i + subarray_length - 1];
        END IF;
    END LOOP;

    -- 返回具有最小歐幾里得距離的子陣列
    RETURN QUERY SELECT best_index, min_euclidean;
END;
$$ LANGUAGE plpgsql;

-- -- define find_min_vshift_euclidean_subarray function
DROP FUNCTION IF EXISTS find_min_vshift_euclidean_subarray(long_array DOUBLE PRECISION[], short_array DOUBLE PRECISION[]);

CREATE OR REPLACE FUNCTION find_min_vshift_euclidean_subarray(long_array DOUBLE PRECISION[], short_array DOUBLE PRECISION[])
RETURNS TABLE (best_index INT, min_vshift_euclidean DOUBLE PRECISION) AS $$
DECLARE
    subarray_length INT;
    min_vshift_euclidean DOUBLE PRECISION := 'infinity';
    current_vshift_euclidean DOUBLE PRECISION;
    best_subarray DOUBLE PRECISION[];
    best_index INT := -1;
    i INT;

BEGIN
    subarray_length := array_length(short_array, 1);

    -- 遍歷 long_array 並計算每個子陣列的 v-shift 歐幾里得距離
    FOR i IN 1..array_length(long_array, 1) - subarray_length + 1 LOOP
        -- 獲取子陣列
        current_vshift_euclidean := vshift_euclidean_distance(long_array[i:i + subarray_length - 1], short_array);

        -- 檢查是否找到更小的 v-shift 歐幾里得距離
        IF current_vshift_euclidean < min_vshift_euclidean THEN
            min_vshift_euclidean := current_vshift_euclidean;
            best_index := i;
            best_subarray := long_array[i:i + subarray_length - 1];
        END IF;
    END LOOP;

    -- 返回具有最小 v-shift 歐幾里得距離的子陣列
    RETURN QUERY SELECT best_index, min_vshift_euclidean;
END;
$$ LANGUAGE plpgsql;
