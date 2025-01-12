import time
from pymongo import MongoClient
import os
import re
import csv

mongo_host = 'localhost'
mongo_port = 27017
mongo_db_name = 'hassan'

mongo_client = MongoClient(mongo_host, mongo_port)
mongo_db = mongo_client[mongo_db_name]

table_names = ['250k', '500k', '750k', '1_million']

queries = [
    ("Q1", {"gender": "Female", "attendance": {"$gt": 80}, "result": "Fail"}),
    ("Q2", {"gender": "Male", "result": "Pass"}),
    ("Q3", {"first_name": {"$regex": "^A"}}),
    ("Q4", {})
]

base_directory = '/Users/hassansaeed/Desktop/DATABASE /Query results/mongodb'
os.makedirs(base_directory, exist_ok=True)

def sanitize_query_description(description):
    return re.sub(r'[^a-zA-Z0-9_]', '', description)

for table_name in table_names:
    collection = mongo_db[table_name]
    print(f"Table: {table_name} DATASET")

    total_time = 0
    execution_times = []

    for query_description, query_filter in queries:
        sanitized_query_description = sanitize_query_description(query_description)

        for i in range(30):
            start_time = time.time()
            result = collection.find(query_filter)
            for document in result:
                pass
            end_time = time.time()
            execution_time = end_time - start_time
            execution_times.append(execution_time)
            total_time += execution_time

            if i == 0:
                print(f"Table: {table_name}, Query: {sanitized_query_description}, First Execution Time: {execution_time} seconds")
            if i == 29:
                avg_execution_time = total_time / 30
                print(f"Table: {table_name}, Query: {sanitized_query_description}, Average Execution Time: {avg_execution_time} seconds")

    filename = os.path.join(base_directory, f"results_{table_name}.csv")
    with open(filename, 'w', newline='') as result_file:
        csv_writer = csv.writer(result_file)
        csv_writer.writerow(['Query', 'Execution Times'])
        for query_num, (query_description, _) in enumerate(queries, start=1):
            sanitized_query_description = sanitize_query_description(query_description)
            response_times = execution_times[query_num - 1::len(queries)]
            csv_writer.writerow([sanitized_query_description] + response_times)

mongo_client.close()
