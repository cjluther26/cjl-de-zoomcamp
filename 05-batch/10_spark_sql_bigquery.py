#!/usr/bin/env python
# coding: utf-8

import argparse 

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

# Removing `.master()` from the SparkSession builder, so that we can run this on a local cluster (instead of just locally)
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

# BigQuery configs
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-east1-640674304618-9daeafn3')



df_green = spark.read.parquet(input_green)


df_yellow = spark.read.parquet(input_yellow)

df_green.printSchema()
# df_green.columns


df_yellow.printSchema()
# df_yellow.columns


##### Get common columns 

# Change datetimes to 'pickup_datetime' and 'dropoff_datetime'
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


common_columns = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]


df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)


df_trips_data.groupBy('service_type').count().show()


# In order to run `spark.sql()`, we need to tell Spark what tables/dataframes exist for querying!

df_trips_data.registerTempTable('trips_data')

spark.sql("""

SELECT 
  service_type
, COUNT(1)
FROM trips_data
GROUP BY 1
-- LIMIT 10;

""").show()


# Now, executing a more complicated query (taking `dim_monthly_zone_revenue.sql` from `04-analytics-engineering`, with some slight adjustments)...
df_result = spark.sql("""
SELECT 
  PULocationID AS revenue_zone 
, DATE_TRUNC('month', pickup_datetime) AS revenue_month
, service_type 

/* Revenues */
, SUM(fare_amount) AS revenue_monthly_fare
, SUM(extra) AS revenue_monthly_extra
, SUM(mta_tax) AS revenue_monthly_mta_tax
, SUM(tip_amount) AS revenue_monthly_tip_amount
, SUM(tolls_amount) AS revenue_monthly_tolls_amount
, SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge
, SUM(total_amount) AS revenue_monthly_total_amount
, SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge

/* Additional Calculations */
, AVG(passenger_count) AS avg_monthly_passenger_count
, AVG(trip_distance) AS avg_monthly_trip_distance
FROM trips_data 
WHERE 1=1
GROUP BY 1,2,3
""")


# # Use `.coalesce()` (which acts like `.repartition()` to REDUCE the number of partitions!)
# # & write to BigQuery
df_result.coalesce(1) \
    .write.format('bigquery') \
    .option('table', output) \
    .save()



