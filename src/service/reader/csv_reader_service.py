import pandas as pd
import os
import sys

# Correct path to import db_connection
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..', 'sql')))

from db_connection import create_connection, close_connection

def read_csv_file(file_path):
    # Check if the file exists
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        return None

    # Read the CSV file
    try:
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        print(f"Error reading the file: {e}")
        return None

def insert_csv_data_to_db(data, connection):
    cursor = connection.cursor()
    insert_query = "INSERT INTO csv_reader (is_consumed, data) VALUES (%s, %s)"

    for _, row in data.iterrows():
        cursor.execute(insert_query, (False, row.to_json()))

    connection.commit()
    print("Data inserted successfully")

if __name__ == "__main__":
    # Hardcoded path to your CSV file
    file_path = "/Users/amanvaidya/Desktop/sample.csv"
    data = read_csv_file(file_path)

    if data is not None:
        # Provide the path to the config file explicitly
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..', 'config.yml'))
        print(f"Config path: {config_path}")  # Debug print
        connection = create_connection(config_path)
        if connection is not None:
            insert_csv_data_to_db(data, connection)
            close_connection(connection)