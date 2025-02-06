## Model deployment
[Tutorial](https://cloud.google.com/bigquery-ml/docs/export-model-tutorial)
### Steps
- Authenticate with GCloud: `gcloud auth login`
- Copy the model from BigQuery into a GCS bucket: `bq --project_id kestra-sandbox-450014 extract -m nytaxi.tip_model gs://kestra-de-zoomcamp-bucket-cjl/taxi_ml_model/tip_model`
- Create a `tmp` directory at root: `mkdir /tmp/model`
- Use `gsutil` to copy the model from the GCS bucket into the new directory you've created: `gsutil cp -r gs://kestra-de-zoomcamp-bucket-cjl/taxi_ml_model/tip_model /tmp/model`
- Move to the directory where you want the serving model to live: `cd ~/Documents/GitHub/cjl-de-zoomcamp/03-data-warehouse`
- Create a directory there: `mkdir -p serving_dir/tip_model/1`
- Copy the model from the `tmp` directory into the `serving_dir`: `cp -r /tmp/model/tip_model/* serving_dir/tip_model/1`
- Pull the `tensorflow` image from Docker: `docker pull tensorflow/serving`
- Deploy the model using the `tensorflow` Docker image: `docker run -p 8501:8501 --mount type=bind,source=`pwd`/serving_dir/tip_model,target=/models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving &`
- Make a POST request (using this `cURL` command or with something like Postman) to make a prediction: `curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' -X POST http://localhost:8501/v1/models/tip_model:predict`
- Check the model status: `http://localhost:8501/v1/models/tip_model`