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
                po_fill_rate_qty_level, 
                po_fill_rate_ucode_level,
                on_time_delivery_rate, 
                damaged_items_received, 
                expired_near_expiry_items_received, 
                pr_count,
                perfect_order_rate 
            FROM vendor_rating 
            WHERE partner_detail_id = %s AND thea_id = %s
          """,  (partner_detail_id, tenant))

        result = cursor.fetchone()

        if not result:
            print("No data found for the given partner_detail_id.")
            return

        # Unpack the result
        po_fill_rate_qty_level, po_fill_rate_ucode_level, on_time_delivery_rate, damaged_items_received, expired_near_expiry_items_received, pr_count, perfect_order_rate = result

        # Use default values or provide the necessary ones for missing fields
        fill_rate_ucode_level = po_fill_rate_ucode_level  # Assuming these are the same, modify if needed
        on_time_delivery_rate = on_time_delivery_rate  # This should be fetched or passed as needed
        perfect_order_rate = perfect_order_rate  # This should be fetched or passed as needed
        near_expiry_expired_items = expired_near_expiry_items_received
        damaged_items = damaged_items_received
        po_fill_rate_qty_level = po_fill_rate_qty_level
        purchase_returns_initiated = pr_count
        # Call gemini_integration with the query results
        html_content = gemini_integration(
            po_fill_rate_qty_level, fill_rate_ucode_level, on_time_delivery_rate,
            perfect_order_rate, near_expiry_expired_items,
            damaged_items, purchase_returns_initiated, partner_detail_id, tenant
        )

        # Print or process the HTML content as needed
        return html_content

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection:
            close_connection(connection)
