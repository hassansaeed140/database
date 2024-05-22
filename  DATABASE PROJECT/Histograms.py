import csv
import matplotlib.pyplot as plt
import numpy as np

def process_data(data):
    first_values = []
    averages = []

    for sublist in data:
        first_value = float(sublist[0]) / 1000  
        first_values.append(first_value)

        remaining_values = [float(value) / 1000 for value in sublist[1:]] 
        average = sum(remaining_values) / len(remaining_values)
        averages.append(average)

    return first_values, averages


def process_neo4j_data(data):
    processed_data = []
    for sublist in data:
        processed_data.append([float(value) / 1000 for value in sublist]) 
    return processed_data


file_mysql = [
    "/Users/hassansaeed/Desktop/DATABASE /Query results/mysql/results_250k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/mysql/results_500k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/mysql/results_750k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/mysql/results_1_million.csv"
]

allValues_mySql = []
for file in file_mysql:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        for _ in range(4):
            next(csv_reader, None)
        second_row = next(csv_reader, None)
        if second_row is not None:
            values = second_row[1:]
            allValues_mySql.append(values)
        else:
            print(f"No data found in {file}")

firstvalue_mySql, average_of_theRest = process_data(allValues_mySql)

# Redis data
allValues_redis = []
file_redis = [
    "/Users/hassansaeed/Desktop/DATABASE /Query results/redis/results_250k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/redis/results_500k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/redis/results_750k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/redis/results_1million.csv"
]
for file in file_redis:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        for _ in range(4):
            next(csv_reader, None)
        second_row = next(csv_reader, None)
        if second_row is not None:
            values = second_row[1:]
            allValues_redis.append(values)
        else:
            print(f"No data found in {file}")

firstvalue_redis, average_of_theRest_redis = process_data(allValues_redis)


allValues_cassandra = []
file_cassandra = [
    "/Users/hassansaeed/Desktop/DATABASE /Query results/Cassandra/results_table_250k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/Cassandra/results_table_500k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/Cassandra/results_table_750k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/Cassandra/results_table_1million.csv"
]
for file in file_cassandra:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        for _ in range(4):
            next(csv_reader, None)
        second_row = next(csv_reader, None)
        if second_row is not None:
            values = second_row[1:]
            allValues_cassandra.append(values)
        else:
            print(f"No data found in {file}")

firstvalue_cassandra, average_of_theRest_cassandra = process_data(allValues_cassandra)

# MongoDB data
allValues_mongodb = []
file_mongodb = [
    "/Users/hassansaeed/Desktop/DATABASE /Query results/mongodb/results_250k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/mongodb/results_500k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/mongodb/results_750k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/mongodb/results_1_million.csv"
]
for file in file_mongodb:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        for _ in range(4):
            next(csv_reader, None)
        second_row = next(csv_reader, None)
        if second_row is not None:
            values = second_row[1:]
            allValues_mongodb.append(values)
        else:
            print(f"No data found in {file}")

firstvalue_mongodb, average_of_theRest_mongodb = process_data(allValues_mongodb)


allValues_neo4j = []
file_neo4j = [
    "/Users/hassansaeed/Desktop/DATABASE /Query results/neo4j/results_250k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/neo4j/results_500k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/neo4j/results_750k.csv",
    "/Users/hassansaeed/Desktop/DATABASE /Query results/neo4j/results_1_million.csv"
]
for file in file_neo4j:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)
        for _ in range(4):
            next(csv_reader, None)
        second_row = next(csv_reader, None)
        if second_row is not None:
            values = second_row[1:]
            allValues_neo4j.append(values)
        else:
            print(f"No data found in {file}")

firstvalue_neo4j, *_ = process_neo4j_data(allValues_neo4j)


dataset_sizes = ["250k", "500k", "750k", "1m"]
databases = ['mySql', 'redis', 'cassandra', 'mongodb', 'neo4j']
response_times = [
    [firstvalue_mySql[0], firstvalue_redis[0], firstvalue_cassandra[0], firstvalue_mongodb[0], firstvalue_neo4j[0]],
    [firstvalue_mySql[1], firstvalue_redis[1], firstvalue_cassandra[1], firstvalue_mongodb[1], firstvalue_neo4j[1]],
    [firstvalue_mySql[2], firstvalue_redis[2], firstvalue_cassandra[2], firstvalue_mongodb[2], firstvalue_neo4j[2]],
    [firstvalue_mySql[3], firstvalue_redis[3], firstvalue_cassandra[3], firstvalue_mongodb[3], firstvalue_neo4j[3]]
]


plt.figure(figsize=(10, 6))
bar_width = 0.15
space_between_bars = 0
index = np.arange(len(response_times[0]))

for i, times in enumerate(response_times):
    plt.bar(index + (bar_width + space_between_bars) * i, times,
            bar_width, alpha=0.7, label=f'Dataset {dataset_sizes[i]}')

plt.xlabel('Dataset')
plt.ylabel('Response Time (s)')
plt.title('Response Time for Different Databases and Datasets')

plt.xticks(index + bar_width * (len(response_times) / 2 - 0.5),
           [f' {databases[i]}' for i in range(len(response_times[0]))])

plt.legend()
plt.grid(True)
plt.show()
