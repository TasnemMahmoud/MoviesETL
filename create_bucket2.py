import os
from google.cloud import storage

def create_bucket(bucket_name):
    """Create a new bucket in the specified location."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.location = 'US'
    try:
        bucket.create()
        print(f"Bucket {bucket_name} created.")
    except Exception as e:
        print(f'Error creating bucket: {e}')

if __name__ == "__main__":
    create_bucket('movies_data_bucket')




