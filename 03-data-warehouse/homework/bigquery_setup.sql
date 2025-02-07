CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-450014.nytaxi.external_yellow_tripdata_hw`
OPTIONS (
  format = 'PARQUET'
, uris = ['gs://kestra-de-zoomcamp-bucket-cjl/yellow_tripdata_2024-*.parquet']
);

/* Create non-partitioned table from external table */
CREATE OR REPLACE TABLE `kestra-sandbox-450014.nytaxi.yellow_tripdata_hw_non_partitioned` AS 
SELECT *
FROM `kestra-sandbox-450014.nytaxi.external_yellow_tripdata_hw`
;

/* Checking the new table! */
SELECT * FROM `kestra-sandbox-450014.nytaxi.yellow_tripdata_hw_non_partitioned` LIMIT 5;