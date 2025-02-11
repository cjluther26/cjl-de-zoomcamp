# DLT Workshop 
## 2025-02-11

## Links
- [YouTube](https://www.youtube.com/watch?v=pgJWP_xqO1g) stream of session
- [GitHub](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/cohorts/2025/workshops/dlt) repo
- [Google Colab Notebook](https://colab.research.google.com/drive/1FiAHNFenM8RyptyTPtDTfqPCi5W6KX_V?usp=sharing) 

## Additional Notes 
### Introduction
- Difference between `dlt` and `dbt`: 
    - `dlt` is a *data load* tool, whereas `dbt` is a *data transformation* tool.

### RESTful APIs
All stuff I've encountered before, no notes (outside of Google Colab notebook) needed.

### `dlt`
- Open-source tool that allows you to extract data from APIs with minimal code, while handling things like pagination, API limits, failures, etc. 

- In this example, we are using the `duckdb` modificator when `pip install`-ing `dlt`. This is because we want to store the data in a lightweight, easy-to-work with database. We could use other databases (Postgres, MySQL, BigQuery), as well.

- `RESTClient` is an object in `dlt` that serves as a framework/construct for all the API requests you'll be making. It needs a `base_url`, but it can handle additional arguments that describe the API you're hitting (i.e. `paginator`, etc.)
```
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

def paginated_getter():
    client = RESTClient(
        base_url = 'https://is-central1-dlthub-analytics-cloudfunctions.net',
        paginator = PageNumberPaginator(
            base_page = 1, 
            total_path = None # go until no data is found on a page
        )
    )

    # Iterate over each page
    for page in client.paginate('data_engineering_zoomcamp_api'): 
        yield page # yield data obtained from API

        # Note that the full URL looks something like: 
        'https://is-central1-dlthub-analytics-cloudfunctions.net/data_engineering_zoomcamp_api/page=100'


    # Now that we have a generator object, we can iterate over it!

for page_data in paginated_getter():
    print(page_data)
    break # for testing
```

- `dlt` [documentation](https://dlthub.com/docs/intro)

### Normalizing data after extraction
- `dlt` handles normalization incredibly well:
    - **automatic** detection of schema (data types, etc.)
    - **flattens** nested JSONs
    - **converts** data types as needed (dates, numbers, booleans, etc.)
    - **splits lists** into child tables
    - **can adapt** to changes in data structure

- When you load the data, you can run `dlt.pipeline.show()` to spin up a simple Streamlit app that shows you exported data, data types, etc.
    - It's very high-level, but provides enough information to be useful!

### Loading the data
- `dlt` handles a lot of things here, too:
    - supports **multiple** destinations (BigQuery, Redshift, Snowflake, Postgres, DuckDB, Parquet, etc.)
    - **optimizes** performance by use case, including batch loading, parallelism, and streaming
    - **maintains** schema
    - **incrementally** loads data by only inserting new or updating existing records
    - automatically handles **failures**, ensuring data is loaded without missing records

- has features for types of loading, such a `scd2` ([documentation](https://dlthub.com/docs/general-usage/incremental-loading#scd2-strategy))


## Homework
I completed the homework assignment (based on the [provided](https://colab.research.google.com/drive/1plqdl33K_HkVx0E0nGJrrkEUssStQsW7) Google Colab notebook) [here](https://colab.research.google.com/drive/1h2jnhnXXMx94Ic5l3nWjl5w4Kxdtr5AP?usp=sharing)


