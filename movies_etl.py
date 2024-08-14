# movies_etl.py

from Helper import google_bucket


google = google_bucket()


google.create_bucket(bucket_name='movies_data_bucket')


ratings = google.process_ratings()
movies = google.process_movies()


google.export_dataframe_to_csv(dataframe=ratings, csv_name='ratings')
google.export_dataframe_to_csv(dataframe=movies, csv_name='movies')


google.load_data(blob_path='first_project/ratings', file_path='ratings.csv', bucket_name='movies_data_bucket')
google.load_data(blob_path='first_project/movies', file_path='movies.csv', bucket_name='movies_data_bucket')
  

