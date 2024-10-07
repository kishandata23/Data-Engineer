# Extract from Postgres and Load to SQL Server

import pandas as pd
import psycopg2
import pyodbc
from sqlalchemy import create_engine
  
# #sql db details
server = "K23\SQLEXPRESS"  

username = 'postgres'
password = 'postgresql'
host = 'localhost'
port = '5432'
database = 'postgres'

#extract from Postgres
def extract():
    try:
        conn = psycopg2.connect(
                    dbname=database,
                    user=username,
                    password=password,
                    host=host,
                    port=port
        )      
        table_name='Ecommerce_Sales_Analysis'
        sql_data=f'''SELECT * FROM public."Ecommerce_Sales_Analysis" ;'''
        src_table = pd.read_sql(sql_data, conn)
        # print(src_table)
        load(src_table,table_name)
        print('Data extracted successfully')
    except Exception as e:
        print('Data extraction error ', e)


# load data to SQL SERVER
def load(src_table,table_name):
   
        connection_string = f'mssql+pyodbc:///?odbc_connect=' + \
                    f'DRIVER={{ODBC Driver 17 for SQL Server}};' \
                    f'SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        engine = create_engine(connection_string)
        try:
            with engine.connect() as conn:
                src_table.to_sql(table_name, con=conn, if_exists='replace', index=False)  # Use 'append' to add to existing table
                print("Data loaded successfully into the database.")
        except Exception as ee:
            
             print('Error in loading data :', ee)


try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))