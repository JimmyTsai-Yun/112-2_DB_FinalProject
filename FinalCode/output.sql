COPY wmt_euclidean_results TO '../csv/wmt_euclidean_results.csv' WITH (FORMAT csv, HEADER);
COPY appl_euclidean_results TO '../csv/appl_euclidean_results.csv' WITH (FORMAT csv, HEADER);
COPY gold_vsift_results TO '../csv/gold_vsift_results.csv' WITH (FORMAT csv, HEADER);
COPY fromch_vsift_results TO '../csv/fromch_vsift_results.csv' WITH (FORMAT csv, HEADER);
COPY gold_war_dtw_results TO '../csv/gold_war_dtw_results.csv' WITH (FORMAT csv, HEADER);