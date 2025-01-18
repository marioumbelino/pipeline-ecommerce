import os
from dotenv import load_dotenv
import mysql.connector
import pandas as pd

def connect_mysql(host_name, user_name, password):
    cnx = mysql.connector.connect(
        host=host_name,
        user=user_name,
        password=password
    )
    
    return cnx

def create_cursor(cnx):
    cursor = cnx.cursor()
    return cursor

def create_database(cursor, db_name):
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name};')
    print('Database created successfully')
    
def show_database(cursor):
    cursor.execute('show databases;')
    
    for db in cursor:
        print(db)
        
def create_table(cursor, database_name, table_name):
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                    id VARCHAR(100),
                    product VARCHAR(100),
                    product_category VARCHAR(100),
                    price FLOAT(10,2),
                    shipping_fee FLOAT(10,2),
                    purchase_date DATE,
                    seller VARCHAR(100),
                    place_of_purchase VARCHAR(100),
                    purchase_evaluation INT,
                    payment_type VARCHAR(100),
                    number_of_installments INT,
                    latitude FLOAT(10,2),
                    longitude FLOAT(10,2),
                    
                    primary key (id));""")
    print('Table created sucessfully')

def show_tables(cursor, db_name):
    cursor.execute(f'use {db_name};')
    cursor.execute('show tables;')
    
    for table in cursor:
        print(table)
        
def read_csv(path):
    return pd.read_csv(path)

def add_product_data(cnx, cursor, dataframe, db_name, table_name):
    data_list = [tuple(row) for index, row in dataframe.iterrows()]
    
    sql = f'INSERT INTO {db_name}.{table_name} (id, latitude, longitude, number_of_installments, payment_type, place_of_purchase, price, product, product_category, purchase_evaluation, seller,shipping_fee, purchase_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    
    cursor.executemany(sql, data_list)
    cnx.commit()
    
    print('Inserted successfully')
    
if __name__ == '__main__':
    load_dotenv()
    
    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    pw = os.getenv('DB_PASSWORD')
    
    cnx = connect_mysql(host, user, pw)

    cursor = create_cursor(cnx)
    
    path = 'data-processed/book_table.csv'
    
    create_database(cursor, 'teste1')
    create_table(cursor, 'teste1', 'table_test2')
    
    show_tables(cursor, 'teste1')
    show_database(cursor)
    
    df = read_csv(path)
    
    add_product_data(cnx, cursor, df, 'teste1', 'table_test1')

    print(f'{cnx} - {cursor}')