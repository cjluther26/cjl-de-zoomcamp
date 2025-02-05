# Homework

1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?
- 128.3 MB
- **134.5 MB**
- 364.7 MB
- 692.6 MB

> I added the following task to `postgres_taxi`
> ```
>   - id: file_output_size
>     type: io.kestra.plugin.core.storage.Size
>     uri: "{{ outputs.extract.outputFiles[inputs.taxi ~ '_tripdata_' ~ inputs.year ~ '-' ~ inputs.month ~ '.csv'] }}"
> ```

2) What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?
- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` 
- **`green_tripdata_2020-04.csv`**
- `green_tripdata_04_2020.csv`
- `green_tripdata_2020.csv`

3) How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?
- 13,537.299
- **24,648,499**
- 18,324,219
- 29,430,127

After uploading the `Yellow` Taxi data for 2020 into Google BigQuery (GBQ), I ran the following SQL query:
```
SELECT 
  REGEXP_EXTRACT(filename, r'\d+') AS year
, COUNT(*) AS num_obs
, COUNT(DISTINCT unique_row_id) AS distinct_obs
FROM `kestra-sandbox-450014.de_zoomcamp.yellow_tripdata`
WHERE 1=1
      AND filename LIKE '%yellow_tripdata_2020%'
GROUP BY 1
```

4) How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?
- 5,327,301
- 936,199
- **1,734,051**
- 1,342,034

After uploading the `Green` Taxi data for 2020 into Google BigQuery (GBQ), I ran the following SQL query:
```
SELECT 
  REGEXP_EXTRACT(filename, r'\d+') AS year
, COUNT(*) AS num_obs
, COUNT(DISTINCT unique_row_id) AS distinct_obs
FROM `kestra-sandbox-450014.de_zoomcamp.green_tripdata`
WHERE 1=1
      AND filename LIKE '%green_tripdata_2020%'
GROUP BY 1
```

5) How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?
- 1,428,092
- 706,911
- **1,925,152**
- 2,561,031

```
SELECT 
  COUNT(*) AS num_obs
FROM `kestra-sandbox-450014.de_zoomcamp.yellow_tripdata_2021-03`
```

6) How would you configure the timezone to New York in a Schedule trigger?
- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration  
- **Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration**
- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- Add a `location` property set to `New_York` in the `Schedule` trigger configuration  

Answer found in Kestra's (documentation)[https://kestra.io/docs/workflow-components/triggers/schedule-trigger#:~:text=A%20schedule%20that%20runs%20daily%20at%20midnight%20US%20Eastern%20time.]