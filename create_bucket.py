import os
from google.cloud import storage


os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/mnt/e/project1/ETL_FILE/myenv/ServiceKey_GoogleCloud.json"

def create_bucket(bucket_name):
    """Create a new bucket in the specified location."""
   
    client = storage.Client()

    
    bucket = client.bucket(bucket_name)
    bucket.location = 'US'

    try:
        
        bucket = client.create_bucket(bucket, location=bucket.location)
        print(f'Bucket {bucket.name} created.')
    except Exception as e:
        print(f'Error creating bucket: {e}')

if __name__ == "__main__":
    create_bucket('bar_data_bucket1')

