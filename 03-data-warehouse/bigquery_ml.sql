-- ------------------------------------------------------------------
-- ------------------------------------------------------------------
-- -- Conducting all of this in GCP Project`kestra-sandbox-450014` --
-- ------------------------------------------------------------------
-- ------------------------------------------------------------------

-- /* 
-- Goal: Build a model that predicts tip amount 

-- I've created the tables used in this demo using the provided queries in the repo!

-- */

-- /* Inspect columns of interest */
-- SELECT 
--   passenger_count
-- , trip_distance
-- , PULocationID
-- , DOLocationID
-- , payment_type
-- , fare_amount
-- , tolls_amount
-- , tip_amount
-- FROM `kestra-sandbox-450014.nytaxi.yellow_tripdata_partitioned` 
-- WHERE 1=1
--       AND fare_amount != 0
-- LIMIT 10
-- ;


-- /* Create ML table with appropriate data types */
-- CREATE OR REPLACE TABLE `kestra-sandbox-450014.nytaxi.yellow_tripdata_ml` (
--   `passenger_count`   INTEGER
-- , `trip_distance`     FLOAT64
-- , `PULocationID`      STRING
-- , `DOLocationID`      STRING
-- , `payment_type`      STRING
-- , `fare_amount`       FLOAT64
-- , `tolls_amount`      FLOAT64
-- , `tip_amount`        FLOAT64
-- ) AS (
--     SELECT 
--       passenger_count
--     , trip_distance
--     , CAST(PULocationID AS STRING)
--     , CAST(DOLocationID AS STRING)
--     , CAST(payment_type AS STRING)
--     , fare_amount
--     , tolls_amount
--     , tip_amount
--     FROM `kestra-sandbox-450014.nytaxi.yellow_tripdata_partitioned` 
--     WHERE 1=1
--           AND fare_amount != 0
-- );

-- /* Create a basic linear regression model using default settings */
-- CREATE OR REPLACE MODEL `kestra-sandbox-450014.nytaxi.tip_model` 
-- OPTIONS (
--     model_type = 'LINEAR_REG'
--   , input_label_cols = ['tip_amount']
--   , DATA_SPLIT_METHOD = 'AUTO_SPLIT'
--   -- /* OPTIONAL - HYPERPARAM TUNING */
--   -- , num_trials = 5
--   -- , max_parallel_trials = 2
--   -- , l1_reg = hparam_range(0, 20)
--   -- , l2_reg=hparam_candidates([0, 0.1, 1, 10])
-- ) AS
--   SELECT *
--   FROM `kestra-sandbox-450014.nytaxi.yellow_tripdata_ml`
--   WHERE 1=1
--         AND tip_amount IS NOT NULL;

-- /* Check model features */
-- SELECT * 
-- FROM ML.FEATURE_INFO(MODEL `kestra-sandbox-450014.nytaxi.tip_model`);


-- /* Evaluate the model */
-- SELECT * 
-- FROM ML.EVALUATE(
--     MODEL `kestra-sandbox-450014.nytaxi.tip_model`
--   , (
--       SELECT *
--       FROM `kestra-sandbox-450014.nytaxi.yellow_tripdata_ml` 
--       WHERE 1=1
--             AND tip_amount IS NOT NULL
--   )
-- );

-- /* Make predictions with the model */
-- SELECT *
-- FROM
-- ML.PREDICT(
--     MODEL `kestra-sandbox-450014.nytaxi.tip_model`
--   , (
--       SELECT *
--       FROM
--       `kestra-sandbox-450014.nytaxi.yellow_tripdata_ml`
--       WHERE 1=1
--             AND tip_amount IS NOT NULL
-- ));

-- /* Make predictions AND explain */
-- SELECT *
-- FROM
-- ML.EXPLAIN_PREDICT(
--     MODEL `kestra-sandbox-450014.nytaxi.tip_model`
--   , (
--       SELECT *
--       FROM
--       `kestra-sandbox-450014.nytaxi.yellow_tripdata_ml`
--       WHERE 1=1
--             AND tip_amount IS NOT NULL
--       LIMIT 10
--   )
--   , STRUCT(3 AS top_k_features)
-- );
/* 

For more information on features available in BigQuery ML, see their documentation: https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-transform

*/

SELECT 1 