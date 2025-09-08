import boto3
from sys import argv
import argparse
import pandas as pd


# load excel file into dataframe
df = pd.read_excel("your_excel_file.xlsx")

# make connection to AWS RDS
import pymysql # or psycopg2
# ... (replace with your RDS details)
connection = pymysql.connect(
    host='your-rds-endpoint.us-east-1.rds.amazonaws.com',
    user='your_username',
    password='your_password',
    database='your_database_name',
    port=3306 # or 5432 for PostgreSQL
)
cursor = connection.cursor() 

# insert SQL statement
# Example for MySQL (adapt for other databases)
table_name = "your_table_name"
columns = ", ".join(df.columns)
placeholders = ", ".join(["%s"] * len(df.columns))
insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

data_to_insert = [tuple(row) for index, row in df.iterrows()]
cursor.executemany(insert_sql, data_to_insert)
connection.commit()

cursor.close()
connection.close()