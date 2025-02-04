## DE Zoomcamp 2.2.1 - Workflow Orchestration Introduction
### What is Kestra?
Kestra allows you to do ETL, batch data pipelines, schedule workflows (time-driven and event-driven), etc. 

Additional notes:
- this is also what Airflow does!
- can you use various coding languages (Python, Rust, R, C#, etc.)
- over 600 plug-ins (cloud platforms, Databricks, Snowflake, dbt, etc.)


## DE Zoomcamp 2.2.2 - Learn Kestra

### Installing Kestra with Docker Compose
The **Learn Kestra** video referenced a few different resources, one of which was a [YouTube](https://www.youtube.com/watch?v=SGL8ywf3OJQ) video on how to install Kestra using Docker Compose. This video walks you through how to use/configure a `docker-compose.yml` file to spin up a Kestra instance tied to a Postgres instance that is *persistent*. 

### Additional resources
- Getting Started with Kestra in 15 Minutes [(Youtube)](https://www.youtube.com/watch?v=a2BZ7vOihjg&t=13s)
- Kestra Tutorial Series [(YouTube Playlist)](https://kestra.io/docs/tutorial)
- Kestra Configuration Guide [(Documentation)](https://kestra.io/docs/configuration)

### Kestra Tutorial Series
I decided to watch the quick YouTube playlist for learning different features in Kestra. Below are my notes!

#### Learn the Fundamentals of Kestra 
- Flows consist of 3 main properties:
    - `id`: unique identifier
    - `namespace`: environment the task will run in
    - `task`: execute an action; need an `id` and `type`
- You can also add things like `description` and `labels` to further document your workflow.

#### Pass Data Into Your Worflows with Inputs
- You can use the `inputs` keyword (best to place it at the top of the workflow) to easily control input variables.
    - Each input receives and `id`, `type`, and `defaults` (what the input defaults to if no argument is provided upon execution).

```
id: getting_started_video
namespace: dev

inputs: 
  - id: api_url
    type: STRING
    defaults: https://dummyjson.com/products

tasks:

  - id: log_task
    type: io.kestra.plugin.core.log.Log
    message: This is another logging statement.
  
  - id: api_get
    type: io.kestra.plugin.core.http.Request
    uri: "{{ inputs.api_url }}"
```

## DE Zoomcamp 2.2.3 - ETL Pipelines with Postgres in Kestra


## DE Zoomcamp 2.2.4 - Manage Scheduling and Backfills with Postgres in Kestra



## DE Zoomcamp 2.2.5 - Orchestrate dbt Models with Postgres in Kestra



## DE Zoomcamp 2.2.6 - ETL Pipelines in Kestra: Google Cloud Platform


## DE Zoomcamp 2.2.7 - Manage Schedules and Backfills with BigQuery in Kestra


## DE Zoomcamp 2.2.8 - Orchestrate dbt Models with BigQuery in Kestra


## DE Zoomcamp 2.2.9 - Deploy Workflows to the Cloud with Git in Kestra