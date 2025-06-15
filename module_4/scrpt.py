import os
import requests
import pandas as pd
from google.cloud import storage
from pyarrow import parquet as pq
from pyarrow import Table

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/data_eng/module_4/creds.json"
BUCKET = os.environ.get("GCP_GCS_BUCKET", "fhv_file")
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.chunk_size = 5 * 1024 * 1024  # 5 MB chunks
    blob.upload_from_filename(local_file)
    print(f"Uploaded to GCS: gs://{bucket.name}/{object_name}")

def web_to_gcs(year, service):
    for i in range(12):
        month = f"{i+1:02d}"
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        request_url = f"{init_url}{service}/{file_name}"

        print(f"Downloading: {request_url}")
        response = requests.get(request_url)
        with open(file_name, "wb") as f:
            f.write(response.content)
        print(f"Saved locally: {file_name}")

        # Read CSV in chunks and enforce consistent types
        parquet_file = file_name.replace('.csv.gz', '.parquet')
        chunk_iter = pd.read_csv(file_name, compression='gzip', chunksize=100_000)
        for idx, chunk in enumerate(chunk_iter):
            # Enforce consistent types
            for col in ['PUlocationID', 'DOlocationID']:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype('float64')
            table = Table.from_pandas(chunk)
            if idx == 0:
                pqwriter = pq.ParquetWriter(parquet_file, table.schema)
            pqwriter.write_table(table)
            print(f"Processed chunk {idx+1}")
        pqwriter.close()
        print(f"Parquet written: {parquet_file}")

        # Upload to GCS
        upload_to_gcs(BUCKET, f"{service}/{parquet_file}", parquet_file)

        # Clean up local files if desired
        os.remove(file_name)
        os.remove(parquet_file)

# Example usage:
web_to_gcs('2019', 'fhv')