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

### Transformations vs. Actions
**Transformations** are **not** executed immediately
- Selecting columns
- Filtering
- Joins 
- `.groupBy()`
- etc.

**Actions** are used to execute transformations
- `.show()`
- `.take()`
- `.head()`
- `.write()`


## 5.3.3 - (Optional) Preparing Yellow and Green Taxi Data

I ran the `download_data.sh` file in Terminal as follows:
- Yellow, 2020: `sh download_data.sh yellow 2020`
- Yellow, 2021:` sh download_data.sh yellow 2021`
- Green, 2020: `sh download_data.sh green 2020`
- Green, 2021:` sh download_data.sh green 2021`

Then, I executed the code in the Jupyter notebook named `05_taxi_schema.ipynb`.

This set me up for the next section!

## 5.3.4 - SQL with Spark

## 5.4.1 - Anatomy of a Spark Cluster

- In a `local` environment, we have executors that carry out jobs. To create a local cluster, 
  we do so when establishing a Spark context (attributing `.master()` with `'local[*]'`).

### Working with a real cluster
We have Spark code on our machine. We submit it to `.master()` (which has a port attached to it, usually 4040, that we can use to see what is being executed on the cluster). 
This provides us an entry point to a Spark cluster, and we can use the Spark **driver** and `spark-submit` to launch the application on a cluster. 
Then, the master distributes the job across multiple **executors**, assigning them tasks as their capabilities allow (and redistribute in the instance where an executor goes down).
Each executor pulls a partition from a DataFrame (with multiple partitions; think of each partition as a `.parquet` file!), process, and save results.
These DataFrames / parquet files typically live in a datalake like Google Cloud Storage or Amazon Web Services S3.

Previously, Hadoop and HDFS were used to store the data "within the executor", or, in other words, download the code to process data TO the machine that ALREADY has the data. 
This was very helpful given that the application used to process data was small relative to the size of the data that needed to be processed. 

But now, the common infrastructure is to have Spark clusters and the data typically live within the same data "center", which removes the need for something like Hadoop/HDFS.

![Spark Cluster Anatomy (From Video)](zoomcamp_spark_cluster_anatomy.png)


## 5.4.2 - GroupBy in Spark
When processing partitioned data, each executor executes a `GROUP BY` *within its own partition*. These intermediate results (let's call them "stage 1") are stored separately and combined in "stage 2".

The key operation to do this in "Stage 2" is called **shuffling**. This redistributes data across executors based on grouping keys (or `JOIN` keys). 

![Spark Group By Example (From Video)](zoomcamp_spark_groupby_example.png)


## 5.4.3 - Joins in Spark

![Spark Join Example (From Video)](zoomcamp_spark_join_example.png)

### Broadcast `JOIN`
When one of the joining tables is small, each executor gets a "copy" of it to execute the `JOIN` (in memory). This eliminates the need for shuffling and is much more efficient, both in terms of computation and time!


## 5.5.1 - (Optional) Operations on Spark RDDs

An **Resilient Distributed Dataset (RDD)** is an immutable distributed collection of elements of your data, partitioned across nodes in your cluster that can be operated in parallel.

They can be leveraged with a low-level API that offers various transformations and actions. 

According to [databricks](https://www.databricks.com/glossary/what-is-rdd), RDDs are especially helpful when:
1. You want low-level transformation/actions and control over your dataset
2. You're working with unstructured data (media streams, text streaming, etc.)
3. You'd rather manipulate data with functional programming constructs, as opposed to domain-specific expressions
4. Its unimportant to impose a schema, such as columnar format while processing or accessing data attributes by name/column 
5. You can forgo some optimization and performance benefits available with DataFrames and Datasets for structured and semi-structured data.

Here are some examples that fit each of these!

#### Low-Level Control 
Example: implementing *custom partitioning*:
```
rdd = sc.parallelize([("A", 1), ("B", 2), ("C", 3), ("D", 4)], numSlices = 2)
partitioned_rdd = rdd.partitionBy(2) # Manually partition into 2 partitions
print(partitioned_rdd.glom().collect()) # View data in each partition

```

#### Handling unstructured/semi-structured data
Example: filtering log files for errors
```
rdd = sc.textFile("logs.txt") # Read unstructured text file
error_rdd = rdd.filter(lambda line: "ERROR" in line) 
print(error_rdd.collect())

```

#### Schema flexibility
Example: processing JSONs with different structures
```
rdd = sc.parallelize([
    '{"user": "Alice", "action": "click"}',
    '{"user": "Bob", "action": "purchase", "amount": 20}',
])

import json
parsed_rdd = rdd.map(lambda x: json.loads(x))  # Parse JSON dynamically
print(parsed_rdd.collect())  # [{'user': 'Alice', 'action': 'click'}, {'user': 'Bob', 'action': 'purchase', 'amount': 20}]

```

#### Complex / stateful computations (custom aggregations)
Example: custom word count with reduce
```
rdd = sc.parallelize(["apple", "banana", "apple", "orange", "banana", "banana"])
word_count = rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
print(word_count.collect()) # [('apple', 2), ('banana', 3), ('orange', 1)]

```

#### Functional programming (no need for SQL-like operations)
Example: mapping and filtering without SQL
```
rdd = sc.parallelize([1, 2, 3, 4, 5, 6])
filtered_rdd = rdd.filter(lambda x: x % 2 == 0) # Keep even numbers
squared_rdd = filtered_rdd.map(lambda x: x ** 2) # Square even numbers
print(squared_rdd.collect()) # [4, 16, 36]

```

## 5.5.2 - (Optional) Spark RDD mapPartition

Example: squaring numbers *in bulk* (`mapPartitions()`) vs. one element at a time (`map()`):
```
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("MapPartitionsExample").getOrCreate()
sc = spark.sparkContext  # Get the SparkContext

# Create an RDD with numbers
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], numSlices=2)  # 2 partitions

# Create function for mapPartitions (which must TAKE an iterator as an arg)
def process_partition(iterator): 
    print("Processing a partition...")
    return (x ** 2 for x in iterator) # Square each number in the partition

# Apply mapPartitions
result_rdd = rdd.mapPartitions(process_partitions)

# Collect results
print(result_rdd.collect()) # Output: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]

```

A **more complex** example: opening a database/API connection ONCE per partition (as opposed to doing so for every record):
```
def query_database(iterator): 
    """
    Simulate DB connection per partition
    """

    print("Opening database connection...")
    results = []
    for record in iterator:
        # Simulating DB lookup
        results.append(f"Processed {record}")
    
    print("Closing database connection...")

    return iter(results) # Return an iterator with results

# Apply mapPartitions
db_result_rdd = rdd.mapPartitions(query_database)

# Collect and print results
print(db_result_rdd.collect())
```
- This would be a **massive performance gain** when dealing with external systems (such as a database connection or API), as it only opens/closes a connection once per partition instead of doing so for *every record*!

## 5.6.1 - Connecting to Google Cloud Storage

To copy files from my repo to GCS, I opened up a new Terminal and executed the following commands:
- `gcloud auth login` (to ensure authentication)
- `gsutil -m cp -r data/pq/ gs://05-batch-spark/data/pq`

Then, we downloaded the Dataproc Cloud Storage Connector by executing the following:
- `mkdir lib` (making a new directory to store the connector)
- `gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar`

## 5.6.2 - Creating a Local Spark Cluster

[Run Spark in Standalone Mode](https://spark.apache.org/docs/latest/spark-standalone.html)

To find where Spark has been installed, and to execute the bash command provided in the link above to initiate a Spark cluster, I ran the following commands in Terminal:
- `which spark-submit` (which returned `/opt/homebrew/Cellar/apache-spark/3.5.4/libexec/bin/spark-submit`)
- `cd /opt/homebrew/Cellar/apache-spark/3.5.4/libexec`
- `./sbin/start-master.sh`
This will initiate a Spark cluster at `localhost:8080`!

We then go back to the notebook and start the Spark session. However, we currently have *no workers*, so we can't actually do anything. Thankfully, the guide linked above tells us what to do!
```
./sbin/start-worker.sh <master-spark-URL> # In my case, this was ./sbin/start-worker.sh spark://MacBook-Pro.lan:7077
```

Great! Now let's turn `10_spark_sql_local_cluster.ipynb` into a Python script...
- Go to the directory that houses the file (in my case, `cjl-de-zoomcamp/05-batch`)
- `jupyter nbconvert --to=script 10_spark_sql_local_cluster.ipynb`
- `python 10_spark_sql_local_cluster.py`
    - I had to make sure `pyspark` was installed and ready to go before doing this
    - Make sure the application started earlier in this module is killed, otherwise there won't be enough resources available to run this!

We then added args parsing to make it a bit more dynamic...
Then, I ran this in Terminal:
```
python 10_spark_sql_local_cluster.py \
    --input_green="data/pq/green/2020/*/" \
    --input_yellow="data/pq/yellow/2020/*/" \
    --output="data/report-2020"
```
    - Note that I had to encase each argument in double-quotes (which the instructor did not have to do!)

We then removed `.master()` from `SparkSession.builder()` and replaced it with `spark-submit` to make this truly run on a local cluster (instead of locally to this specific machine)!

```
URL="spark://MacBook-Pro.lan:7077"
spark-submit \
    --master="${URL}" \
    10_spark_sql_local_cluster.py \
        --input_green="data/pq/green/2021/*/" \
        --input_yellow="data/pq/yellow/2021/*/" \
        --output="data/report-2021"
```
You can also configure workers, resources, etc. 
Please see Apache Spark's [documentation](https://spark.apache.org/docs/latest/submitting-applications.html) for more information!

Before we end this exercise, we have to ensure we're stopping the master and workers!
```
./sbin/stop-worker.sh
./sbin/stop-master.sh
```


## 5.6.3 - Setting up a Dataproc Cluster

After creating a cluster in Dataproc, we uploaded our Python script to the GCS bucket we'll point the cluster to. Note that in order to properly submit this Spark job and leverage the configurations available in Dataproc, we have to ensure that the script doesn't specify `.master()`. In this case, we will be using `10_spark_sql_local_cluster.py`, which we created in the last module.

Using `gsutil`: `gsutil cp 10_spark_sql_local_cluster.py gs://05-batch-spark/code/10_spark_sql_local_cluster.py`

Now, for submitting a job:
- Arguments: 
    `--input_green=gs://05-batch-spark/data/pq/green/2021/*/`
    `--input_yellow=gs://05-batch-spark/data/pq/yellow/2021/*/`
    `--output=gs://05-batch-spark/report-2021`
    - Note that for this, I had to *remove the double quotes* I had to use in the previous local submission. I'm not sure why...

Instead of using the Google Cloud UI, we can also use the Google Cloud SDK. In the job details from what we've just done, there is an *Equivalent REST Response* feature, which shows us how! (More information on how to do this can be found in Google Cloud [documentation](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud), as well).

Using `gcloud`...
```
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=us-east1 \
    gs://05-batch-spark/code/10_spark_sql_local_cluster.py \
    -- \
        --input_green="gs://05-batch-spark/data/pq/green/2020/*/" \
        --input_yellow="gs://05-batch-spark/data/pq/yellow/2020/*/" \
        --output="gs://05-batch-spark/report-2020"
```
- Now for this one, I needed double quotes for the arguments again...what in the world!?


## 5.6.4 - Connecting Spark to BigQuery

Google Cloud also provides an example on how to connect PySpark to BigQuery [directly](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark).

We created a BigQuery-enabled version of the file we've been working with (`10_spark_sql_bigquery.py`) and uploaded it to GCS: `gsutil cp 10_spark_sql_bigquery.py gs://05-batch-spark/code/10_spark_sql_bigquery.py`.

We also reconfigured the `output` argument in our bash command to align with the changes made within `10_spark_sql_bigquery.py` to actually put the final output in a BigQuery table.

```
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=us-east1 \
    gs://05-batch-spark/code/10_spark_sql_bigquery.py \
    -- \
        --input_green="gs://05-batch-spark/data/pq/green/2020/*/" \
        --input_yellow="gs://05-batch-spark/data/pq/yellow/2020/*/" \
        --output="trips_data_all.reports_2020"
```