import pandas as pd
from faker import Faker
import random
 
fake = Faker()

def generate_student_data(num_records):
    student_ids = set()  
    data = {
        'StudentID': [],
        'FirstName': [],
        'LastName': [],
        'Age': [],
        'Grade': [],
        'Attendance': [],
    }
    
    while len(data['StudentID']) < num_records:
        student_id = f'ST{str(len(data["StudentID"]) + 1).zfill(6)}'
        if student_id not in student_ids:
            student_ids.add(student_id)
            data['StudentID'].append(student_id)
            data['FirstName'].append(fake.first_name())
            data['LastName'].append(fake.last_name())
            data['Age'].append(random.randint(18, 25))
            data['Grade'].append(random.randint(60, 100))
            data['Attendance'].append(random.randint(0, 100))

    return pd.DataFrame(data)

total_records = 1000000
df = generate_student_data(total_records)

part1 = df[:250000]
part2 = df[:500000]
part3 = df[:750000]
part4 = df[:1000000]

folder_path = '/Users/hassansaeed/Desktop/database/'

part1.to_csv(f'{folder_path}250k.csv', index=False)
part2.to_csv(f'{folder_path}500k.csv', index=False)
part3.to_csv(f'{folder_path}750k.csv', index=False)
part4.to_csv(f'{folder_path}1_million.csv', index=False)

print("Data generation complete.")
