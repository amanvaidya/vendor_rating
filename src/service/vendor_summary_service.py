import json
import os
import sys
import mysql.connector
from service.gemini.gemini_integration import gemini_integration

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sql')))
from db_connection import create_connection, close_connection

def get_summary(partner_detail_id, tenant):
    try:
        connection = create_connection()
        cursor = connection.cursor()

        # Fetch vendor rating details for the given partner_detail_id
        cursor.execute("""
            SELECT 
                fill_rate, 
                delivery_lead_time, 
                damaged, 
                near_expiry_item, 
                pr_initiated 
            FROM vendor_rating 
            WHERE partner_detail_id = %s AND tenant = %s
          """,  (partner_detail_id, tenant))

        result = cursor.fetchone()

        if not result:
            print("No data found for the given partner_detail_id.")
            return

        # Unpack the result
        fill_rate_qty_level, delivery_lead_time, damaged, near_expiry_item, purchase_returns_initiated = result

        # Use default values or provide the necessary ones for missing fields
        fill_rate_ucode_level = fill_rate_qty_level  # Assuming these are the same, modify if needed
        on_time_delivery_rate = "10"  # This should be fetched or passed as needed
        perfect_order_rate = "7"  # This should be fetched or passed as needed
        near_expiry_expired_items = near_expiry_item
        damaged_items = damaged

        # Call gemini_integration with the query results
        html_content = gemini_integration(
            fill_rate_qty_level, fill_rate_ucode_level, on_time_delivery_rate,
            delivery_lead_time, perfect_order_rate, near_expiry_expired_items,
            damaged_items, purchase_returns_initiated, partner_detail_id, tenant
        )

        # Print or process the HTML content as needed
        return html_content

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection:
            close_connection(connection)
