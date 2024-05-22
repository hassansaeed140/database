import os
import redis
import pandas as pd
import json

redis_host = '127.0.0.1'
redis_port = 6379
redis_password = None 
r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)

csv_directory = '/Users/hassansaeed/Desktop/DATABASE /csv/'

csv_files = [
    '250k.csv',
    '500k.csv',
    '750k.csv',
    '1_million.csv',
]

for idx, csv_file in enumerate(csv_files, start=1):
    key = f'data_set_{idx}'  
    csv_path = os.path.join(csv_directory, csv_file)

    if os.path.exists(csv_path):  # Check if the file exists
        df = pd.read_csv(csv_path)
        data = df.to_dict(orient='records')
        r.set(key, json.dumps(data))
        print(f'Data from {csv_file} inserted into Redis with key: {key}')
    else:
        print(f'Error: File {csv_file} not found at path: {csv_path}')

r.close()

print("Data insertion into Redis complete.")
