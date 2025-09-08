import boto3
from sys import argv
import argparse
import pandas as pd
import mysql.connector
import pymysql
import psycopg2

def load_data_from_excel():
    # load excel file into dataframe
    df = pd.read_excel("your_excel_file.xlsx")
    return df

# make connection to AWS RDS using pymysql
def create_connection0():
    # ... (replace with your RDS details)
    connection2 = pymysql.connect(
        host='your-rds-endpoint.us-east-1.rds.amazonaws.com',
        user='your_username',
        password='your_password',
        database='your_database_name',
        port=3306 # or 5432 for PostgreSQL
    )
    cursor = connection2.cursor() 

# make connection to AWS RDS using mysql.connector
def create_connection():
    connection = mysql.connector.connect(
    host='your-rds-endpoint.us-east-1.rds.amazonaws.com',
    user='your_username',
    password='your_password',
    database='your_database_name',
    port=3306 # or 5432 for PostgreSQL        
    )
    return connection

# create table function
def create_table(connection, table, fields):
    cursor = connection.cursor()
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({fields})')

# insert record function
def insert_record(connection, table, fields, values):
    cursor = connection.cursor()
    cursor.execute(f'INSERT INTO {table} ({fields}) VALUES ({values})')

def update_record(connection, table, field, value, wfield, wvalue):
    cursor = connection.cursor()
    cursor.execute(f'UPDATE {table} SET {field} = {value} WHERE {wfield} = {wvalue}') 

def delete_record(connection, table, wfield, wvalue):
    cursor = connection.cursor()
    cursor.execute(f'DELETE FROM {table} WHERE {wfield} = {wvalue}')
 
def drop_table(connection, table):
    cursor = connection.cursor()
    cursor.execute(f'DROP {table}')

def select_records(connection, table):
    cursor = connection.cursor()
    cursor.execute(f'SELECT * FROM {table}')
    rows = cursor.fetchall()
    for row in rows:
        print(row)

def main():
    connection = create_connection()

# cursor.close()
# connection2.close()