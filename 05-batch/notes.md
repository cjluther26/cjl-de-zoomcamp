# `05-batch` Notes

## 5.1.1 - Introduction to Batch Processing

### Ways of Processing Data

- **Batch**
    - (In context of taxi data): processing all data for January 15th as an isolated entity
    - Can be executed at various frequencies (weekly, daily, hourly, every X minutes, etc.)
    - Technologies:
        - Python scripts
        - SQL
        - Spark
        - Apache Flink
    - Example Workflow 
        - CSVs from datalake -> Python -> SQL (dbt) -> Spark -> Python
- **Streaming**
    - (In context of taxi data): someone hailing and riding a taxi sends multiple events immediately upon their occurence to a database
    - This will be covered in more detail in the next module!

### Advantages of Batch Jobs
- Easy to manage
- Have *retry* properties 
- Scalable (increase/decrease machine size, increase/decrease Spark cluster size, etc.)

### Disadvantages of Batch Jobs
- Delay in execution (primarily concerns the `cron` job, but also the processing time of individual steps)

## 5.1.2 - Introduction to Spark
According to Wikipedia, **Apache Spark** is an open-source unified analytics engine for large-scale data processing. 

Its also *multilanguage* -- it's native operations are executed in Scala (Java), but variants in Python and R exist, too.

**PySpark** (Python) is the preferred way of using Spark in Data Engineering. 

Spark can execute both batch *and* streaming jobs, but this module will focus primarily on its uses in batch jobs. 

### When to Use Spark?
- Example: Parquet files in datalake -> Spark -> Output in Datalake
- If you can express your batch job as SQL, you should! Use something like Hive or Presto/Athena to do so. If more complexity or computing power is needed, moving a job into Spark is advisable. 


## 5.2.1 - (Optional) Installing Spark on Linux

Instead of following this video, I used the guide in the repo for [setting up Spark on MacOS](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/macos.md). 

Once that is set up & tested, I decided to continue forward and get [PySpark](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md) set up, per the repo documentation, as well.
- This took me well over an hour to configure. Locally, everything worked *fine*. Once I moved into this directory and set up a virtual environment, everything was messed up. 
- The primary issue was that the Jupyter notebook continued to reference `openjdk@21`, not `openjdk@11`, despite my attempts to manually download the latter and configure my `PATH`. 
- Through a bunch of trial and error, I was able to configure my `~/.zshrc` file correctly, create a new virtual environment (`.venv`), and get the notebook to run.
- Wish it didn't take so long!!!


## 5.3.1 - First Look at Spark / PySpark

The code used to unzip the `.csv.gz` file crashed my Juypter notebook, so I ran the following code in a new Terminal (while in the directory where the `.csv.gz` file is located):
```
find . -name '*.csv.gz' -exec gzip -d {} \;
```

### Notes
- Spark **does not** infer data types!
    - Running `df.head(5)` in this example, you can see that fields like `pickup_datetime` are initially read as strings
    - We can use `pandas` to our advantage here (create a small dataframe from the data, uses `pd.dtypes` to get data type schema, and manipulate as needed)
        - Use `types` (`from pyspark.sql import types`) to feed schema upon reading data into a Spark dataframe (`spark.read`)
    - Note that `int` data type takes only 4 bytes, whereas `long` data type takes 8 bytes
- Writing to `.parquet` files
    - `ls -lh fhvhv/2021/01/ | wc -l`: used to count the number of files in the directory after running `df.write.parquet('fhvhv/2021/01/')` on our repartitioned DataFrame
    


## 5.3.2 - Spark DataFrames



## 5.3.3 - (Optional) Preparing Yellow and Green Taxi Data



## 5.3.4 - SQL with Spark

## 5.4.1 - Anatomy of a Spark Cluster


## 5.4.2 - GroupBy in Spark


## 5.4.3 - Joins in Spark


## 5.5.1 - (Optional) Operations on Spark RDDs



## 5.5.2 - (Optional) Spark RDD mapPartition


## 5.6.1 - Connecting to Google Cloud Storage


## 5.6.2 - Creating a Local Spark Cluster



## 5.6.3 - Setting up a Dataproc Cluster



## 5.6.4 - Connecting Spark to BigQuery