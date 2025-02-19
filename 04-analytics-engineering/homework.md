# Homework

## Setup
For this homework assignment, the following data was needed:
- `yellow_tripdata`: 2019-2020
- `green_tripdata`: 2019-2020
- `fhv_tripdata`: 2019

I used two separate methods to get this data into BigQuery. In reality, only one was necessary, but I wanted to use both as a means of practice. In both instances, files were uploaded to a Google Cloud Storage (GCS) Bucket (`kestra-de-zoomcamp-bucket-cjl`) in my Google Cloud project `kestra-sandbox-450014`:

1. Kestra -- using advice from the Zoomcamp's Slack, I was able to craft a Kestra flow (saved in this repo as `03-data-warehouse/homework/download_taxi_data_from_github_to_gcs.yaml`) that downloaded `.csv.gz` files from the [DataTalksClub GitHub repo](https://github.com/DataTalksClub/nyc-tlc-data) and, using keys securely saved in Kestra, upload them to GCS. I had tried building a flow that executed this in a loop for green, yellow, and fhv datasets, but it was processing too much and erroring out. Thus, I ran this flow manually one-by-one. 
    - *Note: I had to ensure that the `docker-compose.yml` file in `03-data-warehouse/homework` was up and running!*
2. Using `get_taxi_data.py` in the `03-data-warehouse/homework` directory. This method downloaded `.parquet` files directly from [nyc.gov](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), cleaned necessary fields, and uploaded them to GCS.
    - I manipulated the base variables in the script to grab the data I needed.
    - This method was particularly useful for the **fhv** data, which had 3 fields (`SR_Flag`, `PUlocationID`, and `DOlocationID`) that were throwing data type errors when reading into a table in BigQuery. I had to add a function (`process_parquet()`) to `get_taxi_data.py` to coerce data types before dumping the files into BigQuery.
    

After this, I ran the following DDL queries in BigQuery to create both **external tables** tied to the GCS bucket and **partitioned tables** to store the data. Note that these tables were all stored in a different Google Cloud project named `ny-rides-cjl`:

```
/* Creating external table */
CREATE OR REPLACE EXTERNAL TABLE `ny-rides-cjl.raw.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-de-zoomcamp-bucket-cjl/yellow_tripdata_2019-*.csv', 'gs://kestra-de-zoomcamp-bucket-cjl/yellow_tripdata_2020-*.csv']
);


/* Create partitioned table from external table */
CREATE OR REPLACE TABLE `ny-rides-cjl.taxi_data.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_pickup_datetime) 
CLUSTER BY VendorID
AS 
SELECT *
FROM `ny-rides-cjl.raw.external_yellow_tripdata`
;



-- /* Creating external table */
CREATE OR REPLACE EXTERNAL TABLE `ny-rides-cjl.raw.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-de-zoomcamp-bucket-cjl/green_tripdata_2019-*.csv', 'gs://kestra-de-zoomcamp-bucket-cjl/green_tripdata_2020-*.csv']
);


-- /* Create partitioned table from external table */
CREATE OR REPLACE TABLE `ny-rides-cjl.taxi_data.green_tripdata_partitioned`
PARTITION BY DATE(lpep_pickup_datetime) 
CLUSTER BY VendorID
AS 
SELECT *
FROM `ny-rides-cjl.raw.external_green_tripdata`
;




-- /* Creating external table */
CREATE OR REPLACE EXTERNAL TABLE `ny-rides-cjl.raw.external_fhv_tripdata`(
    dispatching_base_num	STRING
  , pickup_datetime	TIMESTAMP
  , dropOff_datetime	TIMESTAMP
  , PUlocationID	INT64
  , DOlocationID	INT64
  , SR_Flag       INT64
  , Affiliated_base_number	STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-de-zoomcamp-bucket-cjl/fhv_tripdata_2019-*.parquet']
);


-- /* Create partitioned table from external table */
CREATE OR REPLACE TABLE `ny-rides-cjl.taxi_data.fhv_tripdata_partitioned`
PARTITION BY DATE(Pickup_datetime) AS 
SELECT *
FROM `ny-rides-cjl.raw.external_fhv_tripdata`
;
CREATE OR REPLACE VIEW `ny-rides-cjl.raw.view_fhv_tripdata` AS 
SELECT 
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    CAST(PUlocationID AS INT64) AS PUlocationID,
    CAST(DOlocationID AS INT64) AS DOlocationID,
    CAST(SR_FLAG AS INT64) AS SR_Flag,  -- Ensure it converts
    Affiliated_base_number
FROM `ny-rides-cjl.raw.external_fhv_tripdata`;




SELECT 1
```

## Questions
