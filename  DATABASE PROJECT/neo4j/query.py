from neo4j import GraphDatabase
import os
import time
import csv

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "hassan112"

neo4j_queries = [
    ("Query 1", "MATCH (p:d_{dataset}) WHERE p.Gender = 'Female' AND p.Attendance > 80 AND p.Result = 'Fail' RETURN p"),
    ("Query 2", "MATCH (p:d_{dataset}) WHERE p.Gender = 'Male' AND p.Result = 'Pass' RETURN p"),
    ("Query 3", "MATCH (p:d_{dataset}) WHERE p.FirstName STARTS WITH 'A' RETURN p"),
    ("Query 4", "MATCH (p:d_{dataset}) RETURN p")
]

datasets = ['250k', '500k', '750k', '1_million']

base_directory = '/Users/hassansaeed/Desktop/DATABASE /Query results/neo4j'
os.makedirs(base_directory, exist_ok=True)

def execute_query(driver, query, dataset):
    with driver.session() as session:
        query = query.replace("{dataset}", dataset)
        execution_times = []
        for _ in range(30):
            start_time = time.time()
            result = session.run(query)
            end_time = time.time()
            execution_times.append("{:.16f}".format(end_time - start_time))
        return execution_times

with GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD)) as driver:
    for dataset in datasets:
        filename = os.path.join(base_directory, f"results_{dataset}.csv")
        with open(filename, 'a', newline='') as result_file:
            csv_writer = csv.writer(result_file)
            
            # Write header
            csv_writer.writerow(['Query', 'Execution Times'])
            
            for label, query in neo4j_queries:
                execution_times = execute_query(driver, query, dataset)
                # Write query label and execution times
                csv_writer.writerow([label] + execution_times)
