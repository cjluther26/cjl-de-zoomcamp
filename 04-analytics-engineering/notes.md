# `04-analytics-engineering` Notes

## 4.1.1 - Analytics Engineering Basics

ETL vs ELT
- ETL
    - allows for slightly more stable and compliant data analysis
    - higher storage/compute costs
- ELT
    - allows for faster and more flexible data analysis
    - lower cost, lower maintenance

Kimball Dimensional Modeling
- Objective: deliver data that is clear, comprehensible, and timely. 
- Approach: prioritize understandability and query performance over non-redundant data (3NF)

Element of Dimensional Modeling
- Fact Tables
    - measurements, metrics, facts
    - correspond to business *process*
    - think "verbs"
- Dimension Tables
    - correspond to a business *entity*
    - provide context to a business
    - think "nouns"

Architecture of Dimensional Modeling
- Staging 
    - raw data
    - limited access (i.e. not for all parties!)
- Processing
    - taking raw data and transforming it into data models
    - focus on efficiency
    - ensuring quality standards
- Presentation
    - final representation of data
    - exposure to business stakeholders


## 4.1.2 - What is dbt?

**dbt** is a transformation workflow that leverages SQL to implement and deploy analytical code following SWE best practices like modularity, portability, CI/CD, and documentation. 

### How does dbt work?
Each dbt model:
- is a `.sql` file
- has a `SELECT` statement...and doesn't leverage any DDL or DML
- is compiled and ran by dbt

### Two "flavors" of dbt
1. **dbt Core**
    - open-source project for data transformation
    - builds and runs a dbt project (which consists of `.sql` and `.yml` files) 
    - includes SQL compilation logic, macros, and database adapters
    - includes a CLI interface to run dbt commands locally
    - free to use!

2. `**dbt Cloud**
    - SaaS application that is used to develop and manage dbt projects
    - web-based IDE and cloud CLI to develop, run, and test a dbt project
    - managed environments
    - jobs orchestration
    - logging and alerting
    - integrated documentation
    - admin and metadata API
    - semantic layer

### How will the Zoomcamp use dbt?
The DE Zoomcamp will show how to use dbt two ways:
- BigQuery
    - development using **dbt cloud**
    - no local installation of dbt core required!

- Postgres 
    - development using local IDE of your choice (i.e. VS Code)
    - local installation of **dbt core** connecting to a Postgres database
    - running dbt models through the CLI.

It's up to you to determine which to use!

## 4.2.1 - Start Your dbt Project: BigQuery and dbt Cloud

## 4.2.2 - Start Your dbt Project: Postgres and dbt Core (Local)

### Starting a project locally
1. (Optional) If `dbt-core` is not yet installed, activate your virtual environment (`source .venv/bin/activate`) and run `python -m pip install dbt-core dbt-postgres`
2. Create a file in `~/.dbt/` named `profiles.yml`.
    - see [dbt's documentation](https://docs.getdbt.com/docs/core/connect-data-platform/postgres-setup) for additional information.
3. To create a new dbt project:
    - Run `dbt init` and follow the prompts!
4. Ensure that the generated `dbt_project.yml` file aligns with `profiles.yml`, aligning the profile for the newly-generated project with the project!
5. Test the connection by running `dbt debug`.

## 4.3.1 - Build First dbt Models

### Anatomy of a dbt model

Materialization types:
- **Ephemeral**: temporary and exist only for the duration of a single dbt run
- **View**: virtual tables created by dbt that can be queried just like regular tables
- **Table**: physical representations of data that are created and stored in the database
- **Incremental**: allow for efficient updates to existing tables, minimizing data processing and reducing the need for full data refreshes

### The `FROM` clause of a dbt model

**Sources**
- The data loaded to the data warehouse (DWH) that we use as sources for models
- Configuration defined in the `.yml` files in the `models` folder
- Used in conjunction with the *source* macro (`FROM {{ source('staging', 'yellow_tripdata_2021_01')}}`), which will resolve the name to the right schema while building dependencies automatically!
- Freshness can be defined and tested

**Seeds**
- CSV files stored in the repository in the `seeds` folder
- Benefits of version controlling
- Equivalent to a `copy` command
- Recommended for data that doesn't change frequently
- Run seeds by running `dbt seed -s {{ file_name }}` (i.e. insert the seed file name in place of `{{ file_name }}`)

**Ref**
- Macro (`{{ ref('stg_green_tripdata') }}`) that references the underlying tables and views that exist in the DWH
    - `{{ ref('stg_green_tripdata') }}` will get compiled as, in this example, as `"postgres-zoomcamp-gcp"."public"."stg_green_tripdata"`(generally, its `"database_name"."schema_name"."table_name"`)
- Runs the same code in any environment, resolving correct schemas for you
    - abstraction of environment relates to the `dbt_profile` being used in the run.
- Automatic detection of dependencies

### Creating a model
Here are the steps we took to create a dbt model!

Beginning with our `staging` model...
1. Create a folder `models/staging`
2. Create and configure a `schema.yml` file and declare high-level properties (`sources`, and the underlying `name`, `database`, `schema`, and `tables`)
3. Create `.sql` files to generate the models you're building
4. Run `dbt build` to build the models
    - *Note: we had to delete the models contained in the `example` directory (auto-generated by dbt) in order to completely pass a build!*

#### IMPORTANT NOTE
For the dbt project I am using here, I am connecting to Postgres. The Postgres instance I am using is tied to the `docker-compose.yml` file generated for the `03-data-warehouse` homework. I used Kestra flows to upload the data needed for this exercise to that database. The file can be found here: `cjl-de-zoomcamp/03-data-warehouse/homework/docker-compose.yml`. 

I simply opened a new Terminal, moved into the `03-data-warehouse/homework` directory, and ran `docker compose up -d`. This spun up a docker container housing the Postgres instance with all the data I needed. I then configured the `dbt_project.yml` and `schema.yml` files **in this repo** (`04-analytics-engineering`) to align with that Postgres instance!

### Macros
Now, let's cover **macros** so we can implement them into our models. Macros:
- Use control structures (i.e. `IF` statements and `FOR` loops) in SQL
- Use environment variables in the dbt project for production deployments
- Operate on the results of one query to generate *another* query
- Abstract snippets of SQL into reusable macros
    - Analogous to *functions* in most programming languages

- Example:
```
{#
    This macro returns the description of the payment_type
# }

{% macro get_payment_type_description(payment_type) -%}

    case {{ payment_type }}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'EMPTY'
    end 

{-% endmacro %}

```
This would allow us to *inject* the logic above into a query by simply writing...
```
    SELECT 
      {{ get_payment_type_description('payment_type') }} AS payment_type_description
    FROM {{ source('staging,'green_tripdata_2021_01') }}
    WHERE 1=1
          AND vendorid IS NOT NULL

```
...while also allowing us to do so across models without having to rewrite logic!

Creating `get_payment_type_description.sql` in the `macros` directory, then using it in `stg_green_tripdata.sql`, we can run `dbt build --select 'stg_green_tripdata'` to see how the compiler uses the macro:
```
WITH source AS ( 
    SELECT *
    FROM "postgres-zoomcamp-gcp"."public"."green_tripdata"
)

, renamed AS (
    SELECT 
      vendorid 
    , lpep_pickup_datetime 
    , lpep_dropoff_datetime
    , store_and_fwd_flag
    , ratecodeid
    , pulocationid
    , dolocationid
    , passenger_count
    , trip_distance
    , fare_amount
    , extra
    , mta_tax
    , tip_amount
    , tolls_amount
    , ehail_fee
    , improvement_surcharge
    , total_amount
    , case cast( payment_type as integer)
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'EMPTY'
    end AS payment_type_descripted
    , trip_type
    , congestion_surcharge
    FROM source
)

SELECT *
FROM renamed
```

### Packages
**Packages** are like libraries in other programming languages (i.e. `pandas`).
- They are standalone dbt projects, complete with models and macros that tackle a specific problem area
- By adding a package to a dbt project, the package's models and macros will become part of the project (just like `import pandas as pd` makes `pandas` functions available in a Python session)
- You import packages by adding them to a `packages.yml` (create it in the home of the project, if it doesn't exist!) file and running `dbt deps` 
- You can find packages in the [dbt package hub](https://hub.getdbt.com/)

After using `dbt_utils.generate_surrogate_key()` to build a surrogate key, we can run `dbt build --select stg_green_tripdata`. If successful (see the return in Terminal), you can go to the designated schema in Postgres, refresh the *Views*, and you should see `stg_green_tripdata` available!


Now that we've covered the main features in building out the dbt model for `stg_green_tripdata`, let's copy the existing code from the [repo](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/models/staging/stg_green_tripdata.sql) and discuss some of the differences.

### Variables
**Variables** are useful for defining values that can be (or *should be*) used across the project
- Allows us to pass data to models for compilation
- Can be implemented using the `{{ var('...') }}` function
- Can be defined in two ways:
    1. In the `dbt_project.yml` file
        - Example:
          ```
          vars:
            payment_type_values: [1, 2, 3, 4, 5, 6]
          ```
    2. In the command line
        - Example:
          `dbt build --m <model.sql> --var 'is_test_run: false'`, where the following macro is used:
            ```
            { if var('is_test_run', default = true) %}

                LIMIT 100

            {% endif %}
            ```

### Finishing `staging`
We can now finish the script for `stg_green_tripdata.sql` and follow the same methods for `stg_yellow_tripdata.sql` (which is only slightly different).

### Core Layer

We will move a layer up to the `core` layer. 

First, we need to create a **seed** for **taxi zone** information using the `taxi_zone_lookup.csv` file in the [repo](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/seeds/taxi_zone_lookup.csv)! Simply add the csv to the `seeds` directory.

Now, we can build `fact_trips` by `UNION ALL`-ing `stg_green_tripdata` and `stg_yellow_tripdata` together and joining in zone information for pickups and dropoffs (joining `taxi_zone_lookup` twice!). 

Once that is complete, run `dbt build --select +fact_trips+` to run `fact_trips` **as well as** *all upstream and downstream models* related to it! *(Note: this takes a bit of time, due to how large `stg_yellow_tripdata` is).

## 4.3.2 - Testing and Documenting the Project

### Tests
Once we build models, how can we make sure they're correct?

**Tests**
- Assumptions that we make about the data
- Essentially, these function as a `SELECT` SQL query
    - Returning the # of failing records
- Tests are defined on a column in the `.yml` file
- dbt prvodies basic tests to check if the column values are:
    - Unique
    - Non-null
    - Within a range of accepted values
    - A foreign key to another table
- Tests can also be created with custom queries.

Here are a few examples of tests that live in the `.yml` file:
- ```
  - name: payment_type_description
    description: Description of the payment_type code
    tests:
      - accepted_values:
          values: [1, 2, 3, 4, 5]
          severity: warn
  ```

- ```
  - name: pickup_locationid
    description: locationid where the meter was engaged
    tests:
      - relationships:
          to: ref('taxi_zone_lookup')
          field: locationid
          severity: warn
  ```

#### Cross-Database Macros
dbt offers **cross-database macros**, which essentially allow us to perform a given operation in a platform-agnostic fashion, while dbt translates the function upon build based on the database used. 

For example, `dbt.date_trunc()` truncates a datetime/timestamp and is written *here* as `{{ dbt.date_trunc('month', 'pickup_datetime') }}`. The exact operation carried out upon execution depends on the platform:
- if you're using Postgres, this is translated to `DATE_TRUNC('month', pickup_datetime)`
- If you're using BigQuery, this is translated to `TIMESTAMP_TRUNC(CAST(pickup_datetime AS TIMESTAMP), MONTH)`

## 4.4.1 - Deployment Using dbt Cloud

## 4.4.2 - Deployment Using dbt Core (Local)

## 4.5.1 - Visualizing Data with Google Data Studio

## 4.5.2 - Visualizing Data with Metabase