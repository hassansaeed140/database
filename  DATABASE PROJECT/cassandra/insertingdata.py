from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
import pandas as pd
import uuid

cassandra_host = '127.0.0.1'
cassandra_port = 9042
cassandra_username = 'hassan'
cassandra_password = 'your_password'  
auth_provider = PlainTextAuthProvider(username=cassandra_username, password=cassandra_password)
cluster = Cluster([cassandra_host], port=cassandra_port, auth_provider=auth_provider)
session = cluster.connect()

keyspace_name = 'my_keyspace'


table_names = ['table_250k', 'table_500k', 'table_750k', 'table_1million']


session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")

session.execute(f"USE {keyspace_name}")

for table_name in table_names:
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id UUID PRIMARY KEY,
            StudentID TEXT,
            FirstName TEXT,
            LastName TEXT,
            Age INT,
            Grade INT,
            Attendance INT
        )
    """)

def insert_data_from_csv_batch(csv_path, table_name, batch_size=100):
    df = pd.read_csv(csv_path)
    insert_query = session.prepare(f"""
        INSERT INTO {table_name} (id, StudentID, FirstName, LastName, Age, Grade, Attendance)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    batch = None
    for index, row in df.iterrows():
        if batch is None:
            batch = BatchStatement()
        batch.add(insert_query, (uuid.uuid4(), row['StudentID'], row['FirstName'], row['LastName'], int(row['Age']), int(row['Grade']), int(row['Attendance'])))

        if index % batch_size == 0:
            session.execute(batch)
            batch = None

    if batch:
        session.execute(batch)

csv_files = [
    '/Users/hassansaeed/Desktop/DATABASE /csv/250k.csv',
    '/Users/hassansaeed/Desktop/DATABASE /csv/500k.csv',
    '/Users/hassansaeed/Desktop/DATABASE /csv/750k.csv',
    '/Users/hassansaeed/Desktop/DATABASE /csv/1_million.csv'
]

for i, csv_path in enumerate(csv_files):
    table_name = table_names[i]
    print(f"Inserting data from {csv_path} into {table_name}...")
    try:
        insert_data_from_csv_batch(csv_path, table_name)
        print(f"Data insertion into {table_name} complete.")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")


session.shutdown()
cluster.shutdown()
