# Module 1 Homework: Docker & SQL

## Question 1. Understanding docker first run
Run docker with the python:3.12.8 image in an interactive mode, use the entrypoint bash.

What's the version of pip in the image?

```
docker run -it --entrypoint=bash python:3.12.8
pip --version
```

`pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)`

## Question 2. Understanding Docker networking and docker-compose
Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?

```
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

`db:5432` works!
`postgres:5432` works!
`localhost:5432` does not!
`postgres:5433` does not!
`db:5433` does not!

## Prepare Postgres
```
export URL='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz'
python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=green_taxi_trips \
  --url=${URL}
```

## Question 3. Trip Segmentation Count
During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

- Up to 1 mile
- In between 1 (exclusive) and 3 miles (inclusive),
- In between 3 (exclusive) and 7 miles (inclusive),
- In between 7 (exclusive) and 10 miles (inclusive),
- Over 10 miles

Answers:
- 104,802; 197,670; 110,612; 27,831; 35,281
- **104,802; 198,924; 109,603; 27,678; 35,189**
- 104,793; 201,407; 110,612; 27,831; 35,281
- 104,793; 202,661; 109,603; 27,678; 35,189
- 104,838; 199,013; 109,645; 27,688; 35,202

```
WITH oct_2019_data AS (
  SELECT 
  	"VendorID" AS vendor_id
  , u
  , TO_TIMESTAMP(lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') AS lpep_pickup_datetime
  , TO_TIMESTAMP(lpep_dropoff_datetime, 'YYYY-MM-DD HH24:MI:SS') AS lpep_dropoff_datetime
  , trip_distance
  , fare_amount
  , CASE 
      WHEN trip_distance <= 1 THEN '1_mile'
	  WHEN trip_distance > 1 AND trip_distance <= 3 THEN '1_to_3_miles'
	  WHEN trip_distance > 3 AND trip_distance <= 7 THEN '3_to_7_miles'
	  WHEN trip_distance > 7 AND trip_distance <= 10 THEN '7_to_10_miles'
	  WHEN trip_distance > 10 THEN 'more_than_10_miles'
	  ELSE 'error'
	END AS trip_distance_bucket
  , CASE 
      WHEN trip_distance <= 1 THEN 1
	  WHEN trip_distance > 1 AND trip_distance <= 3 THEN 2
	  WHEN trip_distance > 3 AND trip_distance <= 7 THEN 3
	  WHEN trip_distance > 7 AND trip_distance <= 10 THEN 4
	  WHEN trip_distance > 10 THEN 5
	  ELSE 6
	END AS trip_distance_bucket_sort
  FROM green_taxi_trips
  WHERE 1=1
        AND TO_TIMESTAMP(lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') >= DATE('2019-10-01')
	    AND TO_TIMESTAMP(lpep_dropoff_datetime, 'YYYY-MM-DD HH24:MI:SS') < DATE('2019-11-01')
  -- LIMIT 10
)

SELECT
  trip_distance_bucket
, COUNT(*)
FROM oct_2019_data
GROUP BY 1,trip_distance_bucket_sort
ORDER BY trip_distance_bucket_sort
```

## Question 4. Longest trip for each day
Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.

- 2019-10-11
- 2019-10-24
- 2019-10-26
- **2019-10-31**

```
WITH oct_2019_data AS (
  SELECT 
    index
  , "VendorID" AS vendor_id
  , TO_TIMESTAMP(lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') AS lpep_pickup_datetime
  , TO_TIMESTAMP(lpep_dropoff_datetime, 'YYYY-MM-DD HH24:MI:SS') AS lpep_dropoff_datetime
  , trip_distance
  , fare_amount
  , CASE 
      WHEN trip_distance <= 1 THEN '1_mile'
	  WHEN trip_distance > 1 AND trip_distance <= 3 THEN '1_to_3_miles'
	  WHEN trip_distance > 3 AND trip_distance <= 7 THEN '3_to_7_miles'
	  WHEN trip_distance > 7 AND trip_distance <= 10 THEN '7_to_10_miles'
	  WHEN trip_distance > 10 THEN 'more_than_10_miles'
	  ELSE 'error'
	END AS trip_distance_bucket
  , CASE 
      WHEN trip_distance <= 1 THEN 1
	  WHEN trip_distance > 1 AND trip_distance <= 3 THEN 2
	  WHEN trip_distance > 3 AND trip_distance <= 7 THEN 3
	  WHEN trip_distance > 7 AND trip_distance <= 10 THEN 4
	  WHEN trip_distance > 10 THEN 5
	  ELSE 6
	END AS trip_distance_bucket_sort
  FROM green_taxi_trips
  WHERE 1=1
        AND TO_TIMESTAMP(lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') >= DATE('2019-10-01')
        /* Removing this, as we no longer care about rides that took place IN THEIR ENTIRETY in October 2019 */
	    -- AND TO_TIMESTAMP(lpep_dropoff_datetime, 'YYYY-MM-DD HH24:MI:SS') < DATE('2019-11-01')
)

SELECT
  DATE(lpep_pickup_datetime) AS pickup_dt
, MAX(trip_distance) AS max_distance
FROM oct_2019_data
GROUP BY 1
ORDER BY 2 DESC
```

## Question 5. Three biggest pickup zones
Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?

Consider only lpep_pickup_datetime when filtering by date.
- **East Harlem North, East Harlem South, Morningside Heights**
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park

```
WITH trip_data AS (
  SELECT 
    gtt.index
  , "VendorID" AS vendor_id
  , TO_TIMESTAMP(lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') AS lpep_pickup_datetime
  , TO_TIMESTAMP(lpep_dropoff_datetime, 'YYYY-MM-DD HH24:MI:SS') AS lpep_dropoff_datetime
  , trip_distance
  , total_amount
  , zones."Borough" AS borough
  , zones."Zone" AS zone
  FROM green_taxi_trips gtt 
  LEFT JOIN zones
    ON gtt."PULocationID" = zones."LocationID"
  WHERE 1=1
        AND TO_DATE(lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') = DATE('2019-10-18')
)

SELECT
  zone
, COUNT(1) AS num_rides_on_dt
, ROUND(SUM(total_amount)::NUMERIC, 2) AS total_amount_on_dt
FROM trip_data
WHERE 1=1
GROUP BY 1
HAVING SUM(total_amount) > 13000
ORDER BY 3 DESC
```

## Question 6. Largest tip
For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?

*Note: it's tip, not trip*

We need the name of the zone, not the ID.
- Yorkville West
- **JFK Airport**
- East Harlem North
- East Harlem South


```
WITH trip_data AS (
  SELECT 
    gtt.index
  , "VendorID" AS vendor_id
  , TO_TIMESTAMP(lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') AS lpep_pickup_datetime
  , TO_TIMESTAMP(lpep_dropoff_datetime, 'YYYY-MM-DD HH24:MI:SS') AS lpep_dropoff_datetime
  , tip_amount
  , pu_zones."Borough" AS pu_borough
  , pu_zones."Zone" AS pu_zone
  , do_zones."Borough" AS do_borough
  , do_zones."Zone" AS do_zone
  FROM green_taxi_trips gtt 
  INNER JOIN zones AS pu_zones
    ON gtt."PULocationID" = pu_zones."LocationID"
  LEFT JOIN zones AS do_zones
  	ON gtt."DOLocationID" = do_zones."LocationID"
  WHERE 1=1
        AND TO_DATE(lpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS') >= DATE('2019-10-01')
		AND pu_zones."Zone" = 'East Harlem North'
)

SELECT
  *
FROM trip_data
WHERE 1=1
ORDER BY tip_amount DESC
```