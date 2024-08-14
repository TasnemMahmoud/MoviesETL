import os
import pandas as pd
from google.cloud import pubsub_v1

os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/mnt/e/project1/ETL_FILE/myenv/ServiceKey_GoogleCloud.json"

ratings = pd.read_csv('ml-100k/u.data', delimiter='\t', header=None, names=['user_id', 'item_id', 'rating', 'timestamp'])


with open('ml-100k/u.item', 'r', encoding="ISO-8859-1") as read_file:
    counter = 0
    movies_df = pd.DataFrame(columns=['item_id', 'movie_name', 'release_timestamp'])

    
    for line in read_file:
        
        fields = line.split('|')
        item_id, movie_name, release_timestamp = fields[0], fields[1], fields[2]
        movie_name = movie_name[0:len(movie_name) - len(' (1234)')]

        
        line_data = [int(item_id), str(movie_name), release_timestamp]

        
        temp_df = pd.DataFrame(data=[line_data], columns=['item_id', 'movie_name', 'release_timestamp'])
        movies_df = pd.concat([temp_df, movies_df], ignore_index=True)

        counter += 1

    
    movies_df.sort_values(by='item_id', ascending=True, inplace=True)


read_file.close()


ratings.to_csv('ratings.csv', index=False)
movies_df.to_csv('movies.csv', index=False)


client = pubsub_v1.PublisherClient()
topic_path = client.topic_path('etlpython-432317', 'my-topic')


for index, row in ratings.iterrows():
    message = f"{row['user_id']},{row['item_id']},{row['rating']},{row['timestamp']}"
    client.publish(topic_path, message.encode('utf-8'))


for index, row in movies_df.iterrows():
    message = f"{row['item_id']},{row['movie_name']},{row['release_timestamp']}"
    client.publish(topic_path, message.encode('utf-8'))

print("Data published to Pub/Sub topic successfully.")

