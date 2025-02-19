import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time
import pandas as pd


#Change this to your bucket name
BUCKET_NAME = "kestra-de-zoomcamp-bucket-cjl"
FOLDER_NAME = "03-homework"  # If you want to store files in a subfolder

# Use credentials if set, otherwise, default to application default credentials
CREDENTIALS_FILE = "/Users/cjlut/gcloud_keys/kestra-sandbox-450014-creds.json"

if CREDENTIALS_FILE:
    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
else:
    client = storage.Client()


# BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-"
MONTHS = [f"{i:02d}" for i in range(1, 13)] 
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024  

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def process_parquet(file_path):
    print(f"Processing {file_path}...")

    # Read the Parquet file
    df = pd.read_parquet(file_path)

    # Convert ['SR_Flag', 'PUlocationID', 'DOlocationID'] to INT64 if it exists
    for col in ['SR_Flag', 'PUlocationID', 'DOlocationID']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            print(f"'{col}' column converted to INT64.")
        else:
            print(f"'{col}' column not found. Skipping conversion.")
   
    print(f"Converted columns...here are the first few rows:\n{df.head()}")

    # Save the modified Parquet file (overwrite original)
    df.to_parquet(file_path, index=False)

    print(f"Saved updated file: {file_path}")

    return file_path



def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    # file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")
    file_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        processed_file_path = process_parquet(file_path)
        print(f"Converted: {file_path}")
        return processed_file_path
    
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")