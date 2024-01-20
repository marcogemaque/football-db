from google.cloud import storage
import pandas as pd

def save_file_to_storage(bucket_name,file_path_to_upload,object_to_upload):
    """
    Function to store the newly created parquet files
    into our cloud storage to compose the data lake.
    """
    storage_client = storage.Client.from_service_account_json("./.secrets/fifa-project-399416-df12ebf73aeb.json")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path_to_upload)
    print(f"Loading file {file_path_to_upload}...")
    blob.upload_from_string(object_to_upload.to_csv(index=False), 'text/csv')
    print("File loaded.")