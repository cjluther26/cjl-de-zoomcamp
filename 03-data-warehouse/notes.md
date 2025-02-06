# Videos

## 3.1.1 - Data Warehouse and BigQuery

### Creating external tables
```
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
    format = 'CSV',
    uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
)
```
- Using an `EXTERNAL TABLE` here allows us to create a "framework" of a table used to hold data. However, you'll see that the **Long-term storage size** and **Table size** properties are `0 B` -- that is because BigQuery doesn't actually store the data for external tables (that data is *external* to it!)
- The use of `*` (wildcard character) allows us to pull data that follows the given naming convention (with `*` a placeholder)

### Partitioning
- Essentially enables a "folder-like" storage system for a data table, making querying much more efficient.
- Common partitioned fields include various date parts of a given event, or observation (`dt`, `month`, `year`, etc.)
- You can access the properties of a partitioned table like so:
    ```
    SELECT 
    table_name
    , partition_id
    , total_row
    FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
    WHERE 1=1
        AND table_name = 'yellow_tripdata_partitioned'
    ORDER BY partition_id 
    ```
### Clustering
- Tables that use a *clustered column* (user-defined table property that sorts storage blocks based on the values in the clustered columns) that can improve query performance and reduce query costs.
    ```
    CREATE OR REPLACE TABLE `taxi-rides-ny.nytaxi.yellow_tripdata_partitioned_clustered`
    PARTITION BY DATE(tpep_pickup_datetime)
    CLUSTER BY VendorID AS 
    SELECT * FROM `taxi-rides-ny.nytaxi.external_yellow_tripdata`;
    ```


## 3.1.2 - Partitioning and Clustering

### BigQuery Partitions
- Three types of partitions (time-unit, ingestion time, integer range)
    - For time-unit or ingestion time (i.e. `_PARTITIONTIME`), you can use **daily** (default), **hourly**, **monthly**, or **yearly** partitions
- Limit of 4000 partitions

### BigQuery Clustering
- User-specified columns are used to co-locate related data
- The order of the specified column is important, as it determines the sort order of the data
- Clustering improves the performance of **filtered queries** and **aggregateed queries**
- BigQuery allows for up to **4** clustering columns
- *Note: tables that are < 1GB, partitioning and clustering don't yield performance gains. This, plus with increased costs, make them disadvantageous at this scale*

| Partitioning                                         | Clustering                                                                 |
| -----------------------------------------------------| ---------------------------------------------------------------------------|
| Cost is known upfront                                | Cost is *not* known upfront                                                |
| You need partition-level management                  | You need more granularity than partitioning alone allows                   |
| You often filter/aggregate on a single column        | Queries often use filters/aggregations against multiple (specific) columns |
|                                                      | Cardinality in a column (or group of columns) is large                     |


### When would you choose Clustering Over Partitioning?
- Partitioning results in a small amount of data per partition (~ < 1GB)
- Partitioning results in a large number of partitions (i.e. more than the limits on partitioned tables, which is 4,000 in BigQuery)
- Partitioning results in DML operations modifying the majority of partitions in the table frequently

### Automatic reclustering
As data is added to a clustered table, the newly-inserted data can be written to block that contain key ranges that overlap with the key ranges in previously-written blocks.
-  This can weaken the sort property of the table.

To maintain the performance of a clustered table, BigQuery performs **automatic reclustering** to restore the sort property of the table.
- For partitioned tables, clustering is maintained for data within the scope of each partition.

## 3.2.1 - BigQuery Best Practices

### Cost Reduction
- Avoid `SELECT *`
- Price queries before running them
- Use clustered and/or partitioned tables
- Be cautious with streaming inserts
- Materialize query results in stages

### Query Performance
- Filter on partitioned columns
- Denormalize data, when appropriate
- Use nested or repeated columns
- Use external data sources strategically (i.e. don't do it often.)
- Reduce data before doing any `JOIN`s
- Do not treat `WITH` clauses as "prepared" statements
- Avoid oversharding tables
- Avoid JavaScript user-defined functions
- Use approximate aggregation functions (HyperLogLog++)
- Use `ORDER BY` last for query operations
- Use optimal `JOIN` patterns
    - Place the table with the largest number of rows first (A), followed by the table with the fewest rows (B), and then all remaining tables by decreasing size
        - This results in A being distributed evenly, with B being *broadcasted* to all nodes! 


## 3.2.2 - Internals of BigQuery

### "Behind the Scenes"
- BigQuery stores data in Colossus: a cheap, columnar-oriented storage system. This helps keeps storage costs in BigQuery low.
- Jupiter Network: provides approx. 1 TB per second network speed and allows Storage and Compute, which are on different hardware, to communicate with one another.
- Dremel: query execution engine, separating the query in such a way that individual nodes process pieces of it.

### Row vs Column Oriented
- Row-oriented: looks like what you'd see in a standard data table, or in something like a CSV
    - Advantages include:
        - Good for modifying data (i.e. easy to add)
        - Suited well for OLTP
- Column-oriented: rows are split with respect to the columns associated with them. 
    - Advantages include:
        - Faster data aggregation
        - High compression
        - Less disk space
        - Suited well for OLAP
- Example
    - | Name | Gender | Country | Age |
      |------|--------|---------|-----|
      | CJ   | M      | USA     | 27  |
      | Nick | M      | USA     | 25  |

    - Row-oriented: **CJ M USA 27 | Nick M USA 25**
    - Columnar: 
        - Block 1: CJ Nick
        - Block 2: M M
        - Block 3: USA USA
        - Block 4: 27 25



## 3.3.1 - BigQuery Machine Learning

### Pricing
The Free tier includes:
    - 10 GB / month of data storage
    - 1 TB / month of queries processed
    - First 10 GB / month of ML Create Model step

Once demands (and model type/capability) surpasses these thresholds, you'd incur costs. 

### Example: Logistic Regression
See `03-data-warehouse/bigquery_ml.sql`

## 3.3.2 - BigQuery Machine Learning Deployment
See `03-data-warehouse/extract_model.md`
