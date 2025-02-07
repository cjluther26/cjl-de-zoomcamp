

# BigQuery Set-Up
Create an external table using the Yellow Taxi Trip Records. </br>
Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). </br>
> ```
> CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-450014.nytaxi.external_yellow_tripdata_hw`
> OPTIONS (
> format = 'PARQUET'
> , uris = ['gs://kestra-de-zoomcamp-bucket-cjl/yellow_tripdata_2024-*.parquet']
> );
> 
> /* Create non-partitioned table from external table */
> CREATE OR REPLACE TABLE `kestra-sandbox-450014.nytaxi.yellow_tripdata_hw_non_partitioned` AS 
> SELECT *
> FROM `kestra-sandbox-450014.nytaxi.external_yellow_tripdata_hw`
> ;
> 
> /* Checking the new table! */
> SELECT * FROM `kestra-sandbox-450014.nytaxi.yellow_tripdata_hw_non_partitioned` LIMIT 5;
> ```

# Questions
## Question 1:
What is count of records for the 2024 Yellow Taxi Data?
- 65,623
- 840,402
- **20,332,093**
- 85,431,289

> I used the **Preview** feature in the BigQuery UI for this (s.t. I didn't expend any resources by querying the table!)


## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- **0 MB for the External Table and 155.12 MB for the Materialized Table** 
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

> ```
> SELECT 
>   COUNT(DISTINCT PULocationID)
> FROM `kestra-sandbox-450014.nytaxi.external_yellow_tripdata_hw` ;
> -- 0 B
> 
> SELECT 
>   COUNT(DISTINCT PULocationID)
> FROM `kestra-sandbox-450014.nytaxi.yellow_tripdata_hw_non_partitioned` 
> -- 155.12 MB
> 
> /* External tables don't actually store any data, so BigQuery doesn't yet know how much it has to process until execution. */
> ```

## Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
- **BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires** 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

> ```
> SELECT 
>   PULocationID
> FROM `nytaxi.yellow_tripdata_hw_non_partitioned`
> -- 155.12 MB
> ;
> 
> SELECT 
>   PULocationID
> , DOLocationID
> FROM `nytaxi.yellow_tripdata_hw_non_partitioned`
> -- 310.24 MB
> ```

## Question 4:
How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
- **8,333**

> ```
> SELECT 
>   tpep_pickup_datetime
> , tpep_dropoff_datetime
> FROM `nytaxi.yellow_tripdata_hw_non_partitioned`
> WHERE 1=1
>       AND fare_amount = 0
> ```

## Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
- **Partition by tpep_dropoff_datetime and Cluster on VendorID** 
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

> ```
> CREATE OR REPLACE TABLE `kestra-sandbox-450014.nytaxi.yellow_tripdata_hw_partitioned_clustered`
> PARTITION BY DATE(tpep_pickup_datetime)
> CLUSTER BY VendorID
> AS 
>   SELECT *
>   FROM `kestra-sandbox-450014.nytaxi.external_yellow_tripdata_hw`
> ```

We'll partition using the DATE of `tpep_pickup_datetime`, which will create more useful groupings (both in terms of analytics and in processing capabilities).

## Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- **310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

> ```
> SELECT 
>   DISTINCT VendorID
> FROM `kestra-sandbox-450014.nytaxi.yellow_tripdata_hw_non_partitioned`
> WHERE 1=1
>       AND tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
> -- 337.11 MB
> ;
> 
> SELECT 
>   DISTINCT VendorID
> FROM `kestra-sandbox-450014.nytaxi.yellow_tripdata_hw_partitioned_clustered`
> WHERE 1=1
>       AND tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
> -- 26.86 MB
> ```


## Question 7: 
Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- **GCP Bucket**
- Big Table

## Question 8:
It is best practice in Big Query to always cluster your data:
- True
- **False**


## (Bonus: Not worth points) Question 9:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

> It estimates **0 B**! This is because for `SELECT COUNT(*) FROM my_table` statements, BigQuery uses table metadata (which is already stored) -- with no filters or anything associated with these queries (i.e., no `WHERE` clauses or specific column selections), it has the value readily available.