import os
from google.cloud import pubsub_v1
from google.cloud import storage


os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"


client = pubsub_v1.SubscriberClient()
subscription_path = client.subscription_path('etlpython-432317', 'my-subscription')


storage_client = storage.Client()

def upload_to_bucket(blob_name, file_path, bucket_name):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        print(f'Error uploading to bucket: {e}')
        return False

def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack()

def main():
    streaming_pull_future = client.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == "__main__":
    movies_path = 'movies.csv'
    ratings_path = 'ratings.csv'
    upload_to_bucket(blob_name='first_project/movies', file_path=movies_path, bucket_name='movies_data_bucket')
    upload_to_bucket(blob_name='first_project/ratings', file_path=ratings_path, bucket_name='movies_data_bucket')
    main()
