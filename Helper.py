import os
from google.cloud import storage
import pandas as pd

class google_bucket:

    def __init__(self):
        self.client = storage.Client(project='etlpython-432317')

    def create_bucket(self, bucket_name, location='US'):
        """Create a Google Cloud Storage Bucket."""
        bucket = self.client.bucket(bucket_name)
        bucket.location = location
        try:
            bucket.create()
            print('Bucket Created Successfully')
            print('Bucket Details:', vars(bucket))
        except Exception as e:
            print(f'Error creating bucket: {e}')

    def process_ratings(self, file_path='ml-100k/u.data'):
        """Create DataFrame of movie ratings."""
        ratings = pd.read_csv(file_path, delimiter='\t', header=None, names=['user_id','item_id', 'rating' ,'timestamp'])
        return ratings

    def process_movies(self, file_path='ml-100k/u.item'):
        """Create DataFrame of movie names."""
        movies_df = pd.DataFrame(columns=['item_id', 'movie_name', 'release_timestamp'])
        with open(file_path, 'r', encoding="ISO-8859-1") as read_file:
            for line in read_file:
                fields = line.split('|')
                item_id, movie_name, release_timestamp = fields[0], fields[1], fields[2]
                movie_name = movie_name[0:len(movie_name) - len(' (1234)')]
                line_data = [int(item_id), str(movie_name), release_timestamp]
                temp_df = pd.DataFrame(data=[line_data], columns=['item_id', 'movie_name', 'release_timestamp'])
                movies_df = pd.concat([temp_df, movies_df], ignore_index=True)
            movies_df.sort_values(by='item_id', ascending=True, inplace=True)
        return movies_df

    def export_dataframe_to_csv(self, dataframe, csv_name):
        """Export DataFrame to CSV."""
        try:
            dataframe.to_csv(f'{csv_name}.csv', index=False)
        except Exception as e:
            print(f'Error exporting DataFrame to CSV: {e}')

    def load_data(self, blob_path, file_path, bucket_name):
        """Load CSV files to Google Cloud Storage."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        try:
            blob.upload_from_filename(file_path)
            return True
        except Exception as e:
            print(f'Error uploading file: {e}')
            return False
