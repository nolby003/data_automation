# Data Automation Script
# sql_connectors.py
# author: Benjamin Nolan
# Description: SQL Connectors
# date created: 09-09-2025
# date modified: 12-09-2025
# version: 1.0
import config
import mysql.connector
import pymysql
import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
# -------------------------------
# SQL database connectors
# -------------------------------

# -------------------------------
# SQL Alchemy
# -------------------------------
def sqlalch_create_connection():
    try:
        host=config.postgres_host,
        user=config.postgres_user,
        password=config.postgres_pass,
        dbname=config.postgres_db,
        port=config.postgres_port 
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
        # engine = create_engine('mysql+pymysql://user:password@host:port/database_name')
        print("Connection to PostgreSQL successful!")
        return engine
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
# -------------------------------

# -------------------------------
# PostGreSQL using psycopg2
# -------------------------------
def postgres_create_connection():
    try:
        conn = psycopg2.connect(
            host=config.postgres_host,
            database=config.postgres_db,
            user=config.postgres_user,
            password=config.postgres_pass,
            port=config.postgres_port
        )
        print("Connected to AWS RD (PostGreSQL) successfully!")
    except psycopg2.Error as e:
        print(f"Error connecting to AWS RD (PostGreSQL): {e}") 
# -------------------------------        
        
# -------------------------------
# MySQL using pymysql
# -------------------------------
def pymysql_create_connection():
    try:
        # print(config.mysql_host)
        connection = pymysql.connect(
            host=config.mysql_host,
            user=config.mysql_user,
            password=config.mysql_pass,
            database=config.mysql_db,
            port=config.mysql_port # 3306 or 5432 for PostgreSQL    
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            result = cursor.fetchone()
            print("Connected to AWS RDS successfully!")
    except pymysql.Error as e:
        print(f"Error connecting to RDS: {e}")   
# -------------------------------

# -------------------------------
# MySQL using mysql.connector
# -------------------------------
def mysql_create_connection():
    try:
        connection = mysql.connector.connect(
            host=config.mysql_host,
            user=config.mysql_user,
            password=config.mysql_pass,
            dbname=config.mysql_db,
            port=config.mysql_port   
        )
        if connection.is_connected():
            print("Connected to AWS RDS (MySQL) successfully!")
        return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to RDS: {e}")
# -------------------------------

# -------------------------------