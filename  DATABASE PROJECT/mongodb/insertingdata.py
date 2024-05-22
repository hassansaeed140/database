import pandas as pd
from pymongo import MongoClient


mongo_host = 'localhost'
mongo_port = 27017
mongo_db_name = 'hassan'


csv_file_path = '/Users/hassansaeed/Desktop/DATABASE /csv/'

client = MongoClient(mongo_host, mongo_port)
db = client[mongo_db_name]

def insert_data_to_mongo(file_path, collection_name):
    print(f"Reading CSV file from: {file_path}")
    df = pd.read_csv(file_path)
    records = df.to_dict(orient='records')
    db[collection_name].insert_many(records)
    print(f"Data inserted into {collection_name} collection.")

insert_data_to_mongo(f'{csv_file_path}250k.csv', '250k')
insert_data_to_mongo(f'{csv_file_path}500k.csv', '500k')
insert_data_to_mongo(f'{csv_file_path}750k.csv', '750k')
insert_data_to_mongo(f'{csv_file_path}1_million.csv', '1_million')

client.close()
