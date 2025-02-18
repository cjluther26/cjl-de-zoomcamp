{{ config(materialized='view') }}
 
WITH tripdata AS 
(
  SELECT
    *
  , ROW_NUMBER() OVER(PARTITION BY vendorid, tpep_pickup_datetime) AS r
  FROM {{ source('staging','yellow_tripdata_partitioned') }}
  WHERE 1=1
        AND vendorid IS NOT NULL
)

, renamed AS (
    SELECT
    /* IDs */
      {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} AS tripid    
    , {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} AS vendorid
    , {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }} AS ratecodeid
    , {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} AS pickup_locationid
    , {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} AS dropoff_locationid

    /* Timestamps */
    , CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime
    , CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime

    /* Trip Information */
    , store_and_fwd_flag
    , {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} AS passenger_count
    , CAST(trip_distance AS NUMERIC) AS trip_distance

    /* Yellow cabs are always street-hail */
    , 1 AS trip_type

    /* Payment Information */
    , CAST(fare_amount AS NUMERIC) AS fare_amount
    , CAST(extra AS NUMERIC) AS extra
    , CAST(mta_tax AS NUMERIC) AS mta_tax
    , CAST(tip_amount AS NUMERIC) AS tip_amount
    , CAST(tolls_amount AS NUMERIC) AS tolls_amount
    , CAST(0 AS NUMERIC) AS ehail_fee
    , CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge
    , CAST(total_amount AS NUMERIC) AS total_amount
    , COALESCE({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }}, 0) AS payment_type
    , {{ get_payment_type_description('payment_type') }} AS payment_type_description
    FROM tripdata
    WHERE 1=1
        AND r = 1
)

SELECT
  * 
FROM renamed

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

    LIMIT 100

{% endif %}