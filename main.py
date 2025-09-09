# Data Automation Script
# Migrate data from excel or Google sheets to AWS RDS


# Build pipelines excel file - lists all Source files by type (gsheet, excel)
    # Build out table fields and expected data types


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
    # print(folders)
    # print(folder_path)
    return folder_path

inputDirPath = get_realcwd() + '/input'
inputDir = os.listdir(inputDirPath)
outputDirPath = get_realcwd() + '/output'
pipelinesFile = inputDirPath + '/pipelines.xlsx'
excelFileTypes = ['xlsx', 'xls']

# define google sheet info here
# sheetName = 'dim_customers'
# sheetId = '1O4PluV9MdRHJyZZlNCAIGXMfcOJWNFEOML_Z5WyZyk4'
# url = f'https://docs.google.com/spreadsheets/d/{sheetId}/gviz/tq?tqx=out:csv&sheet={sheetName}'

# load pipelines
def load_pipelines():
    try:
        pipelines = pd.read_excel(pipelinesFile, sheet_name='Source')
        # print(pipelines.columns)
        for _, row in pipelines.iterrows():
            if row['type'] == 'gsheet': # for all google sheets
                table = row['sheet']
                sheet = row['sheet']
                sheetId = row['id']
                url = f'https://docs.google.com/spreadsheets/d/{sheetId}/gviz/tq?tqx=out:csv&sheet={sheet}'
                data = pd.read_csv(url)
                toCSV = os.path.join(outputDirPath, f"{sheet}.csv")
                data.to_csv(toCSV, header=True, index=False)
                toExcel = os.path.join(outputDirPath, f"{sheet}.xlsx")
                data.to_excel(toExcel, header=True, index=False)
            elif row['type'] == 'excel': # for all excel sheets
                print('foo')            
    except Exception as e:
        print(f'Error exception: {e}')    

# load data from google sheet
def load_data_from_gsheet(sheetName, sheetId, url):
    try:
        data = pd.read_csv(url)
        return data
    except Excception as e:
        print(f'Error reading google sheet: {e}')    

def import_files():
    files_df = []
    for file in config.inputDir:
        path = str(file)
        file_split = file.split(sep='.')
        if file_split[1] in config.excelFileTypes:
            values = [path]
            files_df.append(values)
            print(f'Added File: {file}')
        else:
            print(f'Invalid filetype: {file_split[1]}')
    print(f'Files df: {files_df}\n')
    return files_df

def read_file_contents(file):
    f = open(file)
    c = f.read()
    print(f'File contents of {file}: \n{c}\n')
    f.close()
    return c                

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
    # connection = create_connection()
    # data = load_data_from_gsheet(sheetName, sheetId, url)
    # fields = data.columns
    # print(data.dtypes)
    load_pipelines()

if __name__ == '__main__':
    main()
# cursor.close()
# connection2.close()