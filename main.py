# Data Automation Script
# main.py
# author: Benjamin Nolan
# Description: main program
# date created: 09-09-2025
# date modified: 09-09-2025
# version: 1.0

# import boto3
from sys import argv
import argparse
import pandas as pd
import mysql.connector
import pymysql
# import psycopg2
import os
from pathlib import Path

# get current working directory
def get_realcwd():  
    pathToHere = Path(__file__)
    parts = list(pathToHere.parts)
    folders = parts[:-1]
    folder_path = str(Path(*folders))
    return folder_path

# Define input and out folders
inputDirPath = get_realcwd() + '/input'
inputDir = os.listdir(inputDirPath)
outputDirPath = get_realcwd() + '/output'

pipelinesFile = inputDirPath + '/pipelines.xlsx'

# Valid excel file types
excelFileTypes = ['xlsx', 'xls']

# load pipelines
# load a list of gsheets listed from the pipelines file
# creates an excel file with all the data
# creates a csv with columns and no data (requires filling in column values on row 2 with python data types)
def load_pipelines():
    try:
        pipelines = pd.read_excel(pipelinesFile, sheet_name='Source')
        for _, row in pipelines.iterrows():
            if row['type'] == 'gsheet': # for all google sheets
                table = row['sheet']
                sheet = row['sheet']
                sheetId = row['id']
                url = f'https://docs.google.com/spreadsheets/d/{sheetId}/gviz/tq?tqx=out:csv&sheet={sheet}'
                data = pd.read_csv(url)
                toCSV = os.path.join(outputDirPath, f"{sheet}.csv")
                empty_df = pd.DataFrame(columns=data.columns)
                empty_df.to_csv(toCSV, header=True, index=False)
                toExcel = os.path.join(outputDirPath, f"{sheet}.xlsx")
                data.to_excel(toExcel, header=True, index=False)
            elif row['type'] == 'excel': # for all excel sheets
                print('foo')            
    except Exception as e:
        print(f'Error exception: {e}')    

# convert pythong datatypes to SQL datatypes
def python_to_sql_conversion(python_type):
    type_map = {
        "int": "BIGINT",
        "int": "INTEGER",
        "float": "FLOAT",
        "object": "VARCHAR",
        "str": "VARCHAR",
        "bool": "BOOLEAN",
        "datetime": "DATETIME",
        "date": "DATE",
    }
    
    # If user passes a dtype or a type object
    if hasattr(python_type, "name"):   # Pandas / NumPy dtype
        key = python_type.name
    elif isinstance(python_type, type):
        key = python_type.__name__
    elif isinstance(python_type, str):
        key = python_type
    else:
        key = str(python_type)
    return type_map.get(key) 

# build pipelines
# load in the csv files with the pythong datatypes
# pass conversion to get SQL datatypes for each field
# build an excel file with each row a create table statement in prep for creating tables on destination RDS
def ready_pipelines():
    
    create_table_statements = []
    output_excel = os.path.join(outputDirPath, 'create_table_statements.xlsx')

    for file in os.listdir(outputDirPath):
        if file.endswith(".csv"):
            file_path = os.path.join(outputDirPath, file)
            df = pd.read_csv(file_path, nrows=2, header=None)

            table_name = os.path.splitext(file)[0]
            columns = df.iloc[0]
            python_types = df.iloc[1]

            sql_cols = []
            for col, py_type in zip(columns, python_types):
                sql_type = python_to_sql_conversion(py_type)
                sql_cols.append(f"{col} {sql_type}")
            
            create_statement = f"CREATE TABLE {table_name} (\n  " + ",\n    ".join(sql_cols) + "\n);"
            create_table_statements.append({"table_name": table_name, "create_statement": create_statement})           

    output_df = pd.DataFrame(create_table_statements)
    output_df.to_excel(output_excel, index=False)

    print(f"✅ All CREATE TABLE statements saved to {output_excel}")

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
    host='bnolan-mysql-temp.cncvguhmaslw.ap-southeast-2.rds.amazonaws.com',
    user='',
    password='',
    database='bnolan-mysql-temp',
    port=3306 # or 5432 for PostgreSQL        
    )
    return connection

# create table function
def create_table(connection, table, fields):
    cursor = connection.cursor()
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({fields})')

    # create table function
def create_table_from_file():
    file_path = os.path.join(outputDirPath, 'create_table_statements.xlsx')
    df = pd.read_excel(file_path, nrows=2, header=None)
    for _, row in df.iterrows():
        statement = row['create_statement']
    cursor = connection.cursor()
    cursor.execute(statement)

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
    # data = load_data_from_gsheet(sheetName, sheetId, url)
    # fields = data.columns
    # print(data.dtypes)
    # Step 1
    # load_pipelines()
    # python_to_sql_conversion(int)
    # Step 2
    # ready_pipelines()
    # Step 3
    connection = create_connection()
    # create_table_from_file()

if __name__ == '__main__':
    main()
# cursor.close()
# connection2.close()
