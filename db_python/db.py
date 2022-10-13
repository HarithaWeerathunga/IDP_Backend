import mysql.connector
from mysql.connector import Error
import pandas as pd



def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")





def write_to_hdb2(id,filename, mask, label):
    connection = create_db_connection("localhost", "root", "root", "hdb")
    print("writing to the db")
    cursor = connection.cursor()
    mySql_insert_query = """INSERT INTO himage_tbl (id, filename, mask, label) 
                                VALUES (%d, %s, %s, %s) """

    record = (id, filename, mask, label)
    cursor.execute(mySql_insert_query, record)

def write_to_hdb(id,filename, mask, label , connection):
    
    print("writing to the db")
    
    cursor = connection.cursor()
    mySql_insert_query = """INSERT INTO himage_tbl (id, filename, mask, label) 
                                VALUES (%d, %s, %s, %s) """

    record = (id, filename, mask, label)
    cursor.execute(mySql_insert_query, record)


    
create_hyperspectral_image_table = """
CREATE TABLE himage_tbl (
  img_id INT PRIMARY KEY,
  img_address VARCHAR(40) NOT NULL,
  label VARCHAR(40) NOT NULL,
  mask VARCHAR(40) NOT NULL
  );
 """



# create_database_query = "CREATE DATABASE hdb"
# connection = create_db_connection("localhost", "root", "root", "hdb") # Connect to the Database

# execute_query(connection, create_hyperspectral_image_table)