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


-- define mixed distance function with weight from pure euclidean distance and v-shift euclidean distance
DROP FUNCTION IF EXISTS find_min_mixed_subarray(long_array DOUBLE PRECISION[], short_array DOUBLE PRECISION[], weight INT[]);

CREATE OR REPLACE FUNCTION find_min_mixed_subarray(long_array DOUBLE PRECISION[], short_array DOUBLE PRECISION[], weight INT[])
RETURNS TABLE (best_index INT, min_mixed DOUBLE PRECISION) AS $$
DECLARE
    subarray_length INT;
    min_mixed DOUBLE PRECISION := 'infinity';
    current_mixed DOUBLE PRECISION;
    best_subarray DOUBLE PRECISION[];
    best_index INT := -1;
    i INT;
    sum_weight INT := 0;

BEGIN
    subarray_length := array_length(short_array, 1);

    sum_weight := weight[1] + weight[2] + weight[3];

    -- 遍歷 long_array 並計算每個子陣列的混合距離
    FOR i IN 1..array_length(long_array, 1) - subarray_length + 1 LOOP
        -- 獲取子陣列
        current_mixed := (weight[1] * euclidean_distance(long_array[i:i + subarray_length - 1], short_array) + weight[2] * vshift_euclidean_distance(long_array[i:i + subarray_length - 1], short_array) + weight[3] * dtw_distance(long_array[i:i + subarray_length - 1], short_array))/sum_weight;

        -- 檢查是否找到更小的混合距離
        IF current_mixed < min_mixed THEN
            min_mixed := current_mixed;
            best_index := i;
            best_subarray := long_array[i:i + subarray_length - 1];
        END IF;
    END LOOP;

    -- 返回具有最小混合距離的子陣列
    RETURN QUERY SELECT best_index, min_mixed;
END;
$$ LANGUAGE plpgsql;