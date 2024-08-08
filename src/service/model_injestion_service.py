import json
import os
import sys
import pandas as pd
import mysql.connector
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import xgboost as xgb
# Adding the path for database connection utilities
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sql')))
from db_connection import create_connection, close_connection

from service.vendor_rating_service import insert_vendor_rating_data


def clean_and_convert(value):
    """Converts a string with commas to a float, if possible, otherwise returns 0."""
    try:
        if isinstance(value, (float, int)):
            return value
        return float(value.replace(',', ''))
    except ValueError:
        return 0


def feed_data_to_model():
    connection = create_connection()

    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT data 
            FROM csv_reader 
            WHERE is_consumed = 0
        """)

        model = xgb.XGBRegressor()
        model.load_model('/Users/amanvaidya/PycharmProjects/vendor_rating/src/service/vendor_performance_model/vendor_performance_6.h5')



        all_result = cursor.fetchall()

        for result in all_result:
            print(result)
            if result is None:
                print("No new data to process.")
                return

            # Assuming `result[0]` is a JSON string containing the CSV data
            data_json = result[0]
            data_dict = json.loads(data_json)

            # Convert the JSON data to a DataFrame
            print("data_dict",data_dict)
            df = pd.DataFrame(data_dict,index=[0])
            df.drop(columns=['vendor_name','partner_detail_id','thea_id'], inplace=True,errors='ignore')

            # Clean and convert the data
            # df = df.applymap(clean_and_convert)

            print("Processed DataFrame:")
            print(df)


            pred_score = model.predict(df)
            print(pred_score)
            data_dict['rating'] = str(pred_score[0])

            insert_vendor_rating_data([data_dict])


            # Mark the data as consumed in the database
            cursor.execute("""
                UPDATE csv_reader 
                SET is_consumed = 1 
                WHERE data = %s
            """, (data_json,))
        connection.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        connection.rollback()
    finally:
        close_connection(connection)

