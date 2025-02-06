/* Used Google BigQuery UI to create a dataset called `nytaxi` in the `kestra-sandbox-450014` project (ensuring that I selected us-east1, the same location as the GCS bucket we're reading the data in from, as the location) */




-- /* Creating external table */
-- CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-450014.nytaxi.external_yellow_tripdata`
-- OPTIONS (
--   format = 'CSV',
--   uris = ['gs://kestra-de-zoomcamp-bucket-cjl/yellow_tripdata_2019-*.csv', 'gs://kestra-de-zoomcamp-bucket-cjl/yellow_tripdata_2020-*.csv']
-- );

-- SELECT * FROM `kestra-sandbox-450014.nytaxi.external_yellow_tripdata` LIMIT 10

-- -- /* Create non-partitioned table from external table */
-- -- CREATE OR REPLACE TABLE `kestra-sandbox-450014.nytaxi.yellow_tripdata_non_partitioned` AS 
-- -- SELECT *
-- -- FROM `kestra-sandbox-450014.nytaxi.external_yellow_tripdata`
-- -- ;

-- /* Create partitioned table from external table */
-- CREATE OR REPLACE TABLE `kestra-sandbox-450014.nytaxi.yellow_tripdata_partitioned`
-- PARTITION BY DATE(tpep_pickup_datetime) AS 
-- SELECT *
-- FROM `kestra-sandbox-450014.nytaxi.external_yellow_tripdata`
-- ;

-- -- /* Create partitioned AND clustered table from external table */
-- -- CREATE OR REPLACE TABLE `kestra-sandbox-450014.nytaxi.yellow_tripdata_partitioned_clustered`
-- -- PARTITION BY DATE(tpep_pickup_datetime) 
-- -- CLUSTER BY VendorID AS 
-- -- SELECT *
-- -- FROM `kestra-sandbox-450014.nytaxi.external_yellow_tripdata`
-- -- ;

/* Inspecting table partitions */
SELECT 
  table_name
, partition_id
, total_rows
FROM `kestra-sandbox-450014.nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE 1=1
      AND table_name = 'yellow_tripdata_partitioned'
ORDER BY partition_id 