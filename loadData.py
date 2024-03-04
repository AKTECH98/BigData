import os
import csv
import psycopg2
from sqlalchemy import create_engine

engine = create_engine('postgresql://localhost/eCommerce')

data_folder = "eCommerce/"

conn = psycopg2.connect(
    host="localhost",
    database="eCommerce",
    user="anshulkiyawat",
    password="123456"
)
cursor = conn.cursor()

for file in os.listdir(data_folder):

    table_name = file.split('.')[0].split('-')[1]

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS """ + table_name + """ (
            event_time TIMESTAMP,
            event_type VARCHAR(255),
            product_id INTEGER,
            category_id BIGINT,
            category_code VARCHAR(255),
            brand VARCHAR(255),
            price NUMERIC,
            user_id BIGINT,
            user_session VARCHAR(255)
        )
    """)

conn.commit()
cursor.close()


cur = conn.cursor()
for file in os.listdir(data_folder):

    table_name = file.split('.')[0].split('-')[1]

    print("----------{}-----------".format(file))

    print("Loading Data to Dataframe")
    file_path = os.path.join(data_folder, file)
    
    with open(file_path, 'r') as file:

        reader = csv.reader(file)
        
        i = -1
        for row in reader:
            i += 1
            if i==0:
                continue

            row[0] = row[0][:-4]
            
            cur.execute("INSERT INTO " + table_name + " (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        
        conn.commit()

    print("{} Data Uploaded to Database".format(file))

cur.close()
conn.close()