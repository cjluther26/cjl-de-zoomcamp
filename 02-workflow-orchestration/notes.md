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

#### Pass Data Between Tasks with Outputs
We're going to GET data from a URL, save it as a CSV file (using Python), and then query it in a DuckDB instance.

```
id: getting_started_video
namespace: dev

description: | 
  # Getting Started
  Let's `write` some **markdown** - [first flow](https://www.youtube.com/watch?v=wUJuyUIKO3c)

labels:
  owner: cj.luther
  project: de-zoomcamp-kestra-tutorial

inputs: 
  - id: api_url
    type: STRING
    defaults: https://dummyjson.com/products

tasks:
  - id: api_get
    type: io.kestra.plugin.core.http.Request
    uri: "{{ inputs.api_url }}"

  - id: python
    type: io.kestra.plugin.scripts.python.Script
    containerImage: python:slim
    beforeCommands:
      - pip install polars
    warningOnStdErr: false
    outputFiles:
      - "products.csv"
    script: | 
      import polars as pl
      data = {{outputs.api_get.body | jq('.products') | first}}
      df = pl.from_dicts(data)
      df.glimpse()
      df.select(['brand', 'price']).write_csv("products.csv")
  
  - id: sqlQuery
    type: io.kestra.plugin.jdbc.duckdb.Query
    inputFiles: 
      in.csv: "{{ outputs.python.outputFiles['products.csv'] }}"
    sql: |
      SELECT 
        brand
      , ROUND(AVG(price), 2) AS avg_price
      FROM read_csv_auto('{{ workingDir }}/in.csv', header = True)
      GROUP BY 1
      ORDER BY 2 DESC;
    store: true
```

#### Schedule Your Workflows with Triggers
```
id: getting_started_triggers
namespace: dev_tutorial

labels:
  owner: cj.luther
  project: de-zoomcamp-kestra-tutorial

tasks:
  - id: hello
    type: io.kestra.plugin.core.log.Log
    message: Hello World! 🚀

triggers:
  - id: schedule_trigger
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 0 1 * *" # Execute at At 00:00 on day-of-month 1

  # Execute when the `getting started_video` flow finishes!
  - id: flow_trigger
    type: io.kestra.plugin.core.trigger.Flow
    conditions: 
      - type: io.kestra.plugin.core.condition.ExecutionFlow
        namespace: dev
        flowId: getting_started_video
```

#### Control Your Orchestration Logic with Flowable Tasks
Flowable tasks allow you to do things like run things in parallel, create subflows, and create conditional branching!

The flow below uses the `ForEach` flowable task type to execute of a list of tasks in parallel.
- The `concurrencyLimit` property with value `0` makes the list of `tasks` to execute in parallel.
- The `values` property defines the list of items to iterate over. 
- The `tasks` property defines the list of tasks to execute for each item in the list. You can access the iteration value using the `{{ taskrun.value }}` variable.

```
id: python_partitions
namespace: dev_tutorial

description: Process partitions of data in parallel

tasks:
  - id: getPartitions
    type: io.kestra.plugin.scripts.python.Script
    taskRunner:
      type: io.kestra.plugin.scripts.runner.docker.Docker
    containerImage: ghcr.io/kestra-io/pydata:latest
    script: |
      from kestra import Kestra
      partitions = [f"file_{nr}.parquet" for nr in range(1, 10)]
      Kestra.outputs({'partitions': partitions})

  - id: processPartitions
    type: io.kestra.plugin.core.flow.ForEach
    concurrencyLimit: 0
    values: '{{ outputs.getPartitions.vars.partitions }}'
    tasks:
      - id: partition
        type: io.kestra.plugin.scripts.python.Script
        taskRunner:
          type: io.kestra.plugin.scripts.runner.docker.Docker
        containerImage: ghcr.io/kestra-io/pydata:latest
        script: | 
          import random
          import time
          from kestra import Kestra

          filename = '{{ taskrun.value }}'
          print(f"Reading and processing partition {filename}")
          nr_rows = random.randint(1, 1000)
          processing_time = random.randint(1, 20)
          time.sleep(processing_time)
          Kestra.counter('nr_rows', nr_rows, {'partition': filename})
          Kestra.timer('processing_time', processing_time, {'partition': filename})
```


#### Handle Errors and Failures
##### Error Handling
By default, a failure of any task will stop the execution and will mark it as failed. For more control over error handling, you can add `errors` tasks, `AllowFailure` tasks, or automatic retries.

`errors` allows you to execute one (or more) actions before terminating the flow (i.e. sending an email, Slack message, etc.).

You can implement error handling at the `flow` or `namespace` level!
- Flow-Level: useful for custom alerting for a specific flow/task. This is accomplished by adding `error` tasks.
- Namespace-Level: useful for sending notificaitons for any failed execution in a given namespace. Allows for centralized error handling. 

##### Flow-Level Error Handling Using `errors`
```
id: error_handling_flow_level
namespace: dev_tutorial

tasks:
  - id: failure_task
    type: io.kestra.plugin.core.execution.Fail

errors:
  - id: alert_on_failure
    type: io.kestra.plugin.notifications.slack.SlackIncomingWebhook
    url: "{{ secret('SLACK_WEBHOOK') }}" # https://hooks.slack.com/services/xyz/xyz/xyz
    payload: |
      {
        "channel": "#alerts",
        "text": "Failure alert for flow {{ flow.namespace }}.{{ flow.id }} with ID {{ execution.id }}"
      }
```


##### Namespace-Level Error Handling Using a Flow Trigger
Kestra recommends enabling a dedicated monitoring workflow with one of the above mentioned notification tasks (i.e. Slack, Microsoft Teams, Email) and a Flow trigger. 

This example sends a Slack alert as soon as any flow in the namespace `company.analytics` is marked either as `FAILED` or `WARNING` (finishes, but with warnings). 

```
id: error_handling_namespace_level
namespace: dev_tutorial

tasks:
  - id: send_slack_message
    type: io.kestra.plugin.notifications.slack.SlackExecution
    url: "{{ secret('SLACK_WEBHOOK') }}"
    channel: "#general"
    executionId: "{{ trigger.executionId }}"

triggers:
  - id: listen
    type: io.kestra.plugin.core.trigger.Flow
    conditions: 
      - type: io.kestra.plugin.core.condition.ExecutionStatus
        in: 
          - FAILED
          - WARNING
      - type: io.kestra.plugin.core.condition.ExecutionNamespace
        namespace: company.analytics
        prefix: true
```

##### Retries
###### Configuring Retries
Each task can be retried a certain number of times, and in a specific way. Use the `retry` property to control this.

The following type of retries are supported: 
- **Constant**: the task is retried every X seconds/minutes/hours/days.
- **Exponential**: the task is retried every X seconds/minutes/hours/days with an *exponential backoff*.
- **Random**: the task is retried every X seconds/minutes/hours/days with a random delay in between each retry attempt.

In this example, we retry a task 5 times up to 1 minute of a total task run duration, with a constant 2-second interval in between each retry attempt. 

```
id: retry_example
namespace: dev_tutorial

tasks:
  - id: fail_four_times
    type: io.kestra.plugin.scripts.shell.Commands
    taskRunner: 
      type: io.kestra.plugin.core.runner.Process
    commands: 
      - 'if [ "{{ taskrun.attemptsCount }}" -eq 4 ]; then exit 0; else exit 1; fi'
    retry: 
      type: constant
      interval: PT2S
      maxAttempt: 5
      maxDuration: PT1M
      warningOnRetry: false
  
errors:
  - id: will_never_run
    type: io.kestra.plugin.core.debug.Return
    format: This will never be executed as retries will fix the issue. 
```

#### Manage Dependencies with Docker
Good to go!


## DE Zoomcamp 2.2.3 - ETL Pipelines with Postgres in Kestra


## DE Zoomcamp 2.2.4 - Manage Scheduling and Backfills with Postgres in Kestra



## DE Zoomcamp 2.2.5 - Orchestrate dbt Models with Postgres in Kestra



## DE Zoomcamp 2.2.6 - ETL Pipelines in Kestra: Google Cloud Platform


## DE Zoomcamp 2.2.7 - Manage Schedules and Backfills with BigQuery in Kestra


## DE Zoomcamp 2.2.8 - Orchestrate dbt Models with BigQuery in Kestra
- Skipped this one for now, as we haven't covered dbt yet!

## DE Zoomcamp 2.2.9 - Deploy Workflows to the Cloud with Git in Kestra

## DE Zoomcamp / Kestra (Additional)
### Installing Kestra on Google Cloud VM
I followed this (tutorial)[https://www.youtube.com/watch?v=qwA7-hm7d2o] to set up Kestra in my GCP project (`ny-rides-cjl`):
  1. Create a VM in Google Cloud
  2. Install Docker
  3. Use a `docker-compose.yml` file from Kestra to download Kestra
  4. Configure the VM's firewall s.t. a username/password would be needed to access the instance (via the External IP)
    - Note: Pasting `{external_ip}/{port_number}` into your browser is how I could access this!
  5. Separate the Postgres database and spin it up in Cloud SQL
    - Selected `PostgreSQL`, named it `kestra-db`, and configured some advanced options s.t. the VM we've created could communicate with this db.
      - Enable a private IP connection (using default features)
      - Data Protection: turn off "Enable deletion protection" as we are *just in sandbox!*
  6. Change the internal storage s.t. its using Google Cloud Storage
    - *Steps 5 & 6 will allow these two things to be isolated from each other (to prevent errors) and allow us to control the size of the internal disk used for Kestra (which could help manage performance and costs).*