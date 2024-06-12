# 112-2 DBMS Final Project
Establish custom time series data similarity comparison functions based on the timescaledb database system.

## Prerequisites
Before you begin, ensure you have met the following requirements:  

<pre><code>python >= 3.8
postgresql >= 14</code></pre>
Make sure your postgresql contain timescaledb extenstion.  

## Datas for demo
Please run the following query to create a hybertable to store the data.
<pre><code>DROP TABLE IF EXISTS company_stock_prices;

CREATE TABLE company_stock_prices (
    id SERIAL ,
    company TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    price NUMERIC NOT NULL
);

DROP TABLE IF EXISTS target_company_stock_prices;

CREATE TABLE target_company_stock_prices (
    id SERIAL ,
    company TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    price NUMERIC NOT NULL
);</code></pre>

After you make sure the table exsist. Please run the following command to write the csv files into database.
<pre><code>python write_datas.py
python write_inputdata.py</code></pre>

## Create custom function
Run the following command to create the time-series data similarity search function.
<pre><code>psql -U username -d database_name -a -f create_distance_function.sql
psql -U username -d database_name -a -f create_findMinDistance_function.sql
psql -U username -d database_name -a -f create_similaritysearch_function.sql
</code></pre>

## Example for the custom function
To use the self-defined function, there are five arguments you need to specify:

| Argument | Data Type | Description |
|:--------:|:---------:|:-----------:|
| Target Name | text | The name of the target company you want to compare. |
| Search Scope | array | The companies you want to include in the comparison. |
| Comparison Range | boolean | Whether to compare the same time interval as the target data. |
| Start Range | timestamp | The starting date of the target company data. |
| End Range | timestamp | The ending date of the target company data. |

Below are some examples of different kinds of similarity search functions:

1. Pure Euclidean
<pre><code>SELECT * FROM calculate_pure_euclidean_with_timerange('NVDA', ARRAY['APPL', 'ABT', 'BAC'], TRUE, '2012-01-01', '2012-01-10'); 
</code></pre>
2. V-shift Euclidean
<pre><code>SELECT * FROM calculate_vshift_with_timerange('APPL', ARRAY['AMD', 'NVDA'], FALSE, '2021-01-01', '2022-12-31');</code></pre>
3. DTW (Dynamic Time Warping)
<pre><code>SELECT * FROM calculate_dtw_with_timerange('GOLD', ARRAY['APPL', 'ABT', 'BAC'], TRUE, '2001-01-01', '2002-03-10');</code></pre>
4. Mixed similarity search  
In this function, you need to add weight(int[]) to specify each similarity method's weight.
<pre><code>SELECT * FROM calculate_mixed_with_timerange('GOLD', ARRAY['APPL', 'ABT', 'BAC'], TRUE, '2001-01-01', '2002-03-10', [1,2,3]);</code></pre>
The result will show the minimum distance for each company and the corresponding time interval.
<!-- | Table Name               | company | min distance | start time | end time |
|--------------------------|:-----:|:-----:|:-----:|:-----:|
| wmt_euclidean_results    |   ✓   |    ✓  |       |       |
| appl_euclidean_results   |   ✓   |    ✓  |       |       |
| gold_vsift_results       |   ✓   |    ✓  |       |       |
| fromch_vsift_results     |   ✓   |    ✓  |       |       | 
| gold_war_dtw_results     |   ✓   |    ✓  |    ✓  |    ✓  |  -->
