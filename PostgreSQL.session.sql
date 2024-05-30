SELECT * 
FROM pg_extension;

CREATE OR REPLACE FUNCTION dtw_distance(ts1 DOUBLE PRECISION[], ts2 DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION AS $$
DECLARE
    n INT;
    m INT;
    i INT;
    j INT;
    cost DOUBLE PRECISION;
    dtw_table DOUBLE PRECISION[][];
BEGIN
    n := array_length(ts1, 1);
    m := array_length(ts2, 1);

    -- 初始化 DTW 矩陣
    IF n IS NULL OR m IS NULL THEN
        RETURN NULL;
    END IF;

    -- 手動初始化矩陣大小及設置初始值為無限大
    dtw_table := ARRAY[]::DOUBLE PRECISION[][];

    FOR i IN 1..n+1 LOOP
        dtw_table := array_append(dtw_table, ARRAY[]::DOUBLE PRECISION[]);
        FOR j IN 1..m+1 LOOP
            dtw_table[i] := array_append(dtw_table[i], 'infinity'::DOUBLE PRECISION);
        END LOOP;
    END LOOP;
    dtw_table[1][1] := 0;

    -- 填充 DTW 矩陣
    FOR i IN 2..n+1 LOOP
        FOR j IN 2..m+1 LOOP
            cost := abs(ts1[i-1] - ts2[j-1]);
            dtw_table[i][j] := cost + LEAST(dtw_table[i-1][j],    -- 插入
                                            dtw_table[i][j-1],    -- 刪除
                                            dtw_table[i-1][j-1]); -- 匹配
        END LOOP;
    END LOOP;

    -- 返回右下角的值，這是 DTW 距離
    RETURN dtw_table[n+1][m+1];
END;
$$ LANGUAGE plpgsql;

SELECT dtw_distance(ARRAY[1,2,3,4,5], ARRAY[2,3,4,5,6]);