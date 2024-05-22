import csv
import mysql.connector
import os


mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = '12345678'
mysql_port = '3306'
mysql_database = 'hassan'

csv_files = [
    '/Users/hassansaeed/Desktop/DATABASE /csv/250k.csv',
    '/Users/hassansaeed/Desktop/DATABASE /csv/500k.csv',
    '/Users/hassansaeed/Desktop/DATABASE /csv/750k.csv',
    '/Users/hassansaeed/Desktop/DATABASE /csv/1_million.csv',
]

connection = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    port=mysql_port,
    database=mysql_database
)

cursor = connection.cursor()

def create_table(table_name, header):
    columns = ', '.join([f"{col} VARCHAR(255)" for col in header])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    cursor.execute(create_table_query)
    connection.commit()

def insert_data_from_csv(table_name, file_path, header):
    create_table(table_name, header) 
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader) 
        insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in header])})"
        
        for row in csv_reader:
            cursor.execute(insert_query, tuple(row))

with open(csv_files[0], 'r') as csvfile:
    csv_reader = csv.reader(csvfile)
    header = next(csv_reader)

for csv_file in csv_files:
    table_name = os.path.splitext(os.path.basename(csv_file))[0]  
    insert_data_from_csv(table_name, csv_file, header)
    connection.commit()

print("Data inserted successfully!")

cursor.close()
connection.close()
