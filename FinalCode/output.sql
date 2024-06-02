COPY wmt_euclidean_results TO '/Users/caichengyun/Desktop/DB/FinalProject/csv/wmt_euclidean_results.csv' WITH (FORMAT csv, HEADER);
COPY appl_euclidean_results TO '/Users/caichengyun/Desktop/DB/FinalProject/csv/appl_euclidean_results.csv' WITH (FORMAT csv, HEADER);
COPY gold_vsift_results TO '/Users/caichengyun/Desktop/DB/FinalProject/csv/gold_vsift_results.csv' WITH (FORMAT csv, HEADER);
COPY fromch_vsift_results TO '/Users/caichengyun/Desktop/DB/FinalProject/csv/fromch_vsift_results.csv' WITH (FORMAT csv, HEADER);
COPY gold_war_dtw_results TO '/Users/caichengyun/Desktop/DB/FinalProject/csv/gold_war_dtw_results.csv' WITH (FORMAT csv, HEADER);