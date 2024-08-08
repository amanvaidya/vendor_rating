import mysql.connector
from mysql.connector import Error
import yaml
import os

def read_db_config(file_path=None):
    if file_path is None:
        # Default path to config.yml in the current directory
        file_path = os.path.join(os.path.dirname(__file__), '..', 'config.yml')

    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['database']

def create_connection(file_path=None):
    config = read_db_config(file_path)
    connection = None
    try:
        connection = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            passwd=config['password'],
            database=config['name']
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def close_connection(connection):
    if connection:
        connection.close()
        print("The connection is closed")