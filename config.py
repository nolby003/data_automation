# Data Automation Script
# config.py
# author: Benjamin Nolan
# Description: Holds all configuration for program. 
# date created: 09-09-2025
# date modified: 12-09-2025
# version: 1.0
import os
from pathlib import Path
import pandas as pd

# Choose Environment
# 1 - Windows
# 2 - Mac/Linux
env = 1

# Choose Source DB
# 1 - MySQL
# 2 - PostGreSQL
# 3 - Amazon Redshift
# 4 - Excel
# 5 - Google Sheets
src_db = 4

# Choose Destination DB
# 1 - MySQL
# 2 - PostGreSQL
# 3 - Amazon Redshift
dest_db = 2

# Fetch Environment variables
if env == 1: # environ.get
  postgres_host = 'bnolan-database-1-postgres.cncvguhmaslw.ap-southeast-2.rds.amazonaws.com' if (os.environ.get('postgres_host')) is None else os.environ.get('postgres_host')
  postgres_user = 'postgres' if (os.environ.get('postgres_user')) is None else os.environ.get('postgres_user')
  postgres_pass = 'js732W9zvw60RW4eVQ7D' if (os.environ.get('postgres_pass')) is None else os.environ.get('postgres_pass')
  postgres_db = 'postgres' if (os.environ.get('postgres_db')) is None else os.environ.get('postgres_db')
  postgres_port = 5432

  mysql_host = 'bnolan-database-1-mysql.cncvguhmaslw.ap-southeast-2.rds.amazonaws.com' if (os.environ.get('mysql_host')) is None else os.environ.get('mysql_host')
  mysql_user = 'admin' if (os.environ.get('mysql_user')) is None else os.environ.get('mysql_user')
  mysql_pass = 'QHH1cpetATLpAnzMqbTi' if (os.environ.get('mysql_pass')) is None else os.environ.get('mysql_pass')
  mysql_db = '' if (os.environ.get('mysql_db')) is None else os.environ.get('mysql_db')
  mysql_port = 3306
elif env == 2: # getenv
  postgres_host = 'bnolan-database-1-postgres.cncvguhmaslw.ap-southeast-2.rds.amazonaws.com' if (os.getenv('postgres_host')) is None else os.getenv('postgres_host')
  postgres_user = 'postgres' if (os.getenv('postgres_user')) is None else os.getenv('postgres_user')
  postgres_pass = 'js732W9zvw60RW4eVQ7D' if (os.environ.get('postgres_pass')) is None else os.environ.get('postgres_pass')
  postgres_db = 'postgres' if (os.getenv('postgres_db')) is None else os.getenv('postgres_db')
  postgres_port = 5432

  mysql_host = 'bnolan-database-1-mysql.cncvguhmaslw.ap-southeast-2.rds.amazonaws.com' if (os.getenv('mysql_host')) is None else os.getenv('mysql_host')
  mysql_user = 'admin' if (os.getenv('mysql_user')) is None else os.getenv('mysql_user')
  mysql_pass = 'QHH1cpetATLpAnzMqbTi' if (os.getenv('mysql_pass')) is None else os.getenv('mysql_pass')
  mysql_db = '' if (os.getenv('mysql_db')) is None else os.getenv('mysql_db')
  mysql_port = 3306

# params: {
#   # 1 - Excel
#   # 2 - Google Sheets
#   # 3 - postgres
#   # 4 - MySQL
#   # 5 - RedShift
#   # 6 - Oracle
#   src_db_type: 2,
#   # 1 - postgres
#   # 2 - MySQK
#   # 3 - Redshift
#   # 4 - Oracle
#   dest_db_type: 1
#   }

# get current working directory
def get_realcwd():  
    pathToHere = Path(__file__)
    parts = list(pathToHere.parts)
    folders = parts[:-1]
    folder_path = str(Path(*folders))
    return folder_path

def init_checks():
  # ensure input and out folders exist
  input_dir = get_realcwd() + '/input'
  output_dir = get_realcwd() + '/output'
  pipelines_file = get_realcwd() + '/input/pipelines.xlsx'
  if not os.path.exists(input_dir):
    os.makedirs(input_dir)
  if not os.path.exists(output_dir):
    os.makedirs(output_dir)
  if not os.path.exists(pipelines_file):
    with pd.ExcelWriter(pipelines_file) as writer:
          output_df = pd.DataFrame(columns=['type', 'sheet', 'id'])
          output_df.to_excel(writer, sheet_name='Source', header=True, index=False)
  
init_checks()  

# Define input and out folders
inputDirPath = get_realcwd() + '/input'
inputDir = os.listdir(inputDirPath)
outputDirPath = get_realcwd() + '/output'

pipelinesFile = inputDirPath + '/pipelines.xlsx'

# Valid excel file types
excelFileTypes = ['xlsx', 'xls']