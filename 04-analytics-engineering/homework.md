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

# Questions

## Question 1: Understanding dbt model resolution
Provided you've got the following `sources.yaml`:
```
version: 2

sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```
...with the following `env` variables setup where `dbt` runs:
```
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```

... what does this `.sql` model compile to?
```
SELECT * 
FROM {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```

- `SELECT * FROM dtc_zoomcamp_2025.raw_nyc_tripdata.ext_green_taxi`
- `SELECT * FROM dtc_zoomcamp_2025.my_nyc_tripdata.ext_green_taxi`
- **`SELECT * FROM myproject.raw_nyc_tripdata.ext_green_taxi`**
- `SELECT * FROM myproject.my_nyc_tripdata.ext_green_taxi`
- `SELECT * FROM dtc_zoomcamp_2025.raw_nyc_tripdata.green_taxi`

The `sources.yaml` file receives one stored variable (`DBT_BIGQUERY_PROJECT`) from the environment (`myproject`), but because there is no stored value for `DBT_BIGQUERY_SOURCE_DATASET`, dbt will default to the value defined in the `sources.yaml` file for that variable (`raw_nyc_tripdata`)

## Question 2: dbt Variables & Dynamic Models
We have to modify the following dbt model `fact_recent_taxi_trips.sql` such that Analytics Engineers are able to dynamically control the date range. 
- Development: process only the *last 7 days*
- Production: process the *last 30 days*
    ```
    SELECT *
    FROM {{ ref('fact_taxi_trips') }}
    WHERE 1=1
          AND pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
    ```
What would you change to accomplish this in such a way that command line arguments take precedence over `env_var`s, which takes precedence over default values?

- Add `ORDER BY pickup_datetime DESC` and `LIMIT {{ var("days_back", 30) }}`
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", 30) }}' DAY`
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", "30") }}' DAY`
- **Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`**
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", var("days_back", "30")) }}' DAY`

Using `'{{ var("days_back", env_var("DAYS_BACK", "30")) }};` allows us to inject a value to use for the date subtraction with the following precedence:
- `var("days_back")`: variable specified in the run/build
- `env_var("DAYS_BACK")`: stored environment variable
- `30`: a default value if the other two are not found!

## Question 3: dbt Data Lineage & Execution
Considering the data lineage below **and** that `taxi_zone_lookup` is the only materialization build (from a `.csv` seed file):

![image](./homework_q2.png)

Select the option that does **NOT** apply for materializing `fact_taxi_monthly_zone_revenue`: 

- `dbt run`
- `dbt run --select +models/core/dim_taxi_trips.sql+ --target prod`
- `dbt run --select +models/core/fact_taxi_monthly_zone_revenue.sql`
- `dbt run --select +models/core/`
- **`dbt run --select models/staging/+`**

`dbt run --select models/staging/+` will run all models in the `staging` directory and any downstream dependencies. However, it will not run any models in other directories like `core` which, based on the color-coding in the image, is where `fact_taxi_monthly_zone_revenue` is located. So, using this command **will not** materialize `fact_taxi_monthly_zone_revenue`!

## Question 4: dbt Macros & Jinja
Consider you're dealing with sensitive data (i.e. PII) that is only available to your team and few other *carefully selected* individuals in the `raw` layer of the data warehouse.

- Among other things, you decide to obfuscate/masquerade that dadta through `staging` models, making it available there for other Data/Analytics Engineers.
- And optionally, another `service` layer where both `dim` and `fact` tables are available for dashboarding and product teams. 

You decide to make a macro to wrap logic around this data:
```
{% macro resolve_schema_for(model_type) -%}

  {% set target_env_var = 'DBT_BIGQUERY_TARGET_DATASET' -%}
  {% set stging_env_var = 'DBT_BIGQUERY_STAGING_DATASET' -%}

  {%- if model_type == 'core' -%} {{- env_var(target_env_var) -}}
  {%- else -%}                    {{- env_var(stging_env_var, env_var(target_env_var)) -}}
  {%- endif -%}

{%- endmacro %}

```
...and use on your `staging`, `dim` and `fact` models as:
```
{{ config(
     schema=resolve_schema_for('core'),
) }}
```
Select all statements that are true to the models using it:
- **Setting a value for `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile**
- Setting a value for `DBT_BIGQUERY_STAGING_DATASET` env var is mandatory, or it'll fail to compile
- **When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`**
- **When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`**
- **When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`**

Explanation: 
- `DBT_BIGQUERY_TARGET_DATASET` is referred to in each condition, and unless a default value is provided in its definition, it needs to be specified. 
- This is **not true** because the given macro has a default value to use if `DBT_BIGQUERY_STAGING_DATASET` is not provided.
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`, which in this case, will be `core`!
- In the last two options, in both cases, the builds will first look for a value for `DBT_BIGQUERY_STAGING_DATASET` and if one is not found, then the value for `DBT_BIGQUERY_TARGET_DATASET` will be used.

## Question 5: Taxi Quarterly Revenue Growth
1. Create a new model `fact_taxi_trips_quarterly.sql`
2. Compute the quarterly revenue for each year based on `total_amount`
3. Compute the quarterly YoY revenue growth
  - e.g.: In 2020/Q1, Green Taxis had -12.34% revenue growth compared to 2019/Q1
  - e.g.: In 2020/Q4, Yellow Taxis had +34.56% revenue growth compared to 2019/Q4

Considering the YoY growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green & yellow taxis, repectively?

- green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q3, worst: 2020/Q4}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- **green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}**
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q3, worst: 2020/Q4}

The script I used in my dbt project to build `fact_taxi_trips_quarterly` can be found [here](https://github.com/cjluther26/cjl-de-zoomcamp/blob/main/04-analytics-engineering-cloud/taxi_rides_ny/models/core/fact_taxi_trips_quarterly_revenue.sql). After running that in my production environment (which placed the table in `ny-rides-cjl.prod` in BigQuery), it was a pretty simple query!
```
SELECT 
  *
, SAFE_DIVIDE(SAFE_SUBTRACT(quarterly_revenue, last_quarterly_revenue), last_quarterly_revenue) AS percent_change
FROM `ny-rides-cjl.prod.fact_taxi_trips_quarterly_revenue`
WHERE 1=1
ORDER BY service_type, percent_change
```

## Question 6: P97/P95/P90 Taxi Monthly Fare
1. Create a new model `fact_taxi_trips_monthly_fare_p95.sql`
2. Filter out any invalid entries (`fare_amount > 0`, `trip_distance > 0`, and `payment_type_description IN ('Cash', 'Credit card')`)
3. Compute the continuous percentile of `fare_amount`, partitioning by `service_type`, `year`, and `month`.

Now, what are the values of `p97`, `p95`, and `p90` in April 2020 for green and yellow taxis, respectively?

- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}
- **green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}**
- green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}
- green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 25.5, p90: 19.0}

The script I used in my dbt project to build `fact_taxi_trips_monthly_fare_p95` can be found [here](https://github.com/cjluther26/cjl-de-zoomcamp/blob/main/04-analytics-engineering-cloud/taxi_rides_ny/models/core/fact_taxi_trips_monthly_fare_p95.sql). After running that in my production environment (which placed the table in `ny-rides-cjl.prod` in BigQuery), it was a pretty simple query!

```
SELECT * 
FROM `ny-rides-cjl.prod.fact_taxi_trips_monthly_fare_p95` 
WHERE 1=1
      AND pickup_year = 2020
      AND pickup_month = 4
ORDER BY service_type,1,2
```

## Question 7: Top #Nth longest P90 travel time Location for FHV
Prerequisites: 
- Create a `staging` model for FHV data (2019) and **do not** add a deduplication step! Just filter out entries were `dispatching_base_num IS NOT NULL`
- Create a `core` model for FHV data (`fact_fhv_trips.sql`) joining with `dim_zones` 
- Add a few dimensions for `year` and `month`, based on `pickup_datetime`

Now...
1. Create a new model `fact_fhv_monthly_zone_traveltime_p90.sql`
2. For each record in `fact_fhv_trips.sql`, compute the `TIMESTAMP_DIFF()` in seconds between `dropoff_datetime` and `pickup_datetime`, calling it `trip_duration`
3. Compute the continuous `p90` of `trip_duration`, partitioning by `year`, `month`, `pickup_location_id`, and `dropoff_location_id`

For the trips that respectively started from `Newark Airport`, `SoHo`, and `Yorkville East`, in November 2019, what are `dropoff_zone`s with the 2nd-longest `p90` `trip_duration`?

- **LaGuardia Airport, Chinatown, Garment District**
- LaGuardia Airport, Park Slope, Clinton East
- LaGuardia Airport, Saint Albans, Howard Beach
- LaGuardia Airport, Rosedale, Bath Beach
- LaGuardia Airport, Yorkville East, Greenpoint

The scripts I used in my dbt project for this question:
-  `stg_fhv_tripdata` can be found [here](https://github.com/cjluther26/cjl-de-zoomcamp/blob/main/04-analytics-engineering-cloud/taxi_rides_ny/models/staging/stg_fhv_tripdata.sql)
-  `fact_fhv_trips` can be found [here](https://github.com/cjluther26/cjl-de-zoomcamp/blob/main/04-analytics-engineering-cloud/taxi_rides_ny/models/core/fact_fhv_trips.sql)
  - *Note: I did dedupe this here, on the surrogate key (`tripid`) generated in `stg_fhv_tripdata`*
-  `fact_fhv_monthly_zone_traveltime_p90` can be found [here](https://github.com/cjluther26/cjl-de-zoomcamp/blob/main/04-analytics-engineering-cloud/taxi_rides_ny/models/core/fact_fhv_monthly_zone_traveltime_p90.sql)
  - *Note: There were no instructions on sample size, so pickup-dropoff combinations with as little as 1 ride were included!*

After running those in my production environment (which placed tables in `ny-rides-cjl.prod` in BigQuery), it was a pretty simple query!

```
WITH final_data AS (
  SELECT 
    pickup_year
  , pickup_month
  , pickup_zone
  , dropoff_zone 
  , num_obs 
  , num_distinct_obs
  , min_trip_duration_sec
  , avg_trip_duration_sec
  , p50
  , p90
  , p95
  , p97
  , max_trip_duration_sec
  , ROW_NUMBER() OVER(PARTITION BY pickup_Year, pickup_month, pickup_zone ORDER BY p90 DESC) AS p90_r_desc
  FROM final_data 
  WHERE 1=1
        AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
        AND pickup_year = 2019
        AND pickup_month = 11
)

SELECT *
FROM final 
WHERE 1=1
      AND p90_r_desc = 2
```