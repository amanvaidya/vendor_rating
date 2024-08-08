import json
import os
import sys
import mysql.connector

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sql')))
from db_connection import create_connection, close_connection

def update_vendor_rating_matrix(cursor, partner_detail_id, created_on, data, tenant):
    try:
        # Fetch existing matrix data
        cursor.execute("SELECT matrcis FROM vendor_rating_matrix WHERE partner_detail_id = %s AND tenant = %s", (partner_detail_id, tenant))
        matrix_row = cursor.fetchone()

        # Initialize matrix_data with new keys if no existing data is found
        matrix_data = json.loads(matrix_row[0]) if matrix_row else {
            "vendor_rating": [],
            "fill_rating_qty_level": [],
            "fill_rating_ucode_level": [],
            "on_time_delivery_rating": [],
            "median_lead_time_rating": [],
            "perfect_order_rating": [],
            "damaged_rating": [],
            "near_expiry_rating": [],
            "pr_initiated_rating": []
        }

        # Format the created_on date
        date = created_on.strftime('%Y-%m-%d %H:%M:%S')

        # Append new data to the respective fields
        matrix_data['vendor_rating'].append({"rating": data.get('rating'), "date": date})
        matrix_data['fill_rating_qty_level'].append({"fill_rate_qty_level": data.get('po_fill_rate_qty_level'), "date": date})
        matrix_data['fill_rating_ucode_level'].append({"fill_rate_ucode_level": data.get('po_fill_rate_ucode_level'), "date": date})
        matrix_data['on_time_delivery_rating'].append({"on_time_delivery": data.get('on_time_delivery_rate'), "date": date})
        matrix_data['median_lead_time_rating'].append({"median_lead_time": data.get('median_lead_time'), "date": date})
        matrix_data['perfect_order_rating'].append({"perfect_order": data.get('perfect_order_rate'), "date": date})
        matrix_data['damaged_rating'].append({"damaged": data.get('damaged_items_received'), "date": date})
        matrix_data['near_expiry_rating'].append({"near_expiry": data.get('expired_near_expiry_items_received'), "date": date})
        matrix_data['pr_initiated_rating'].append({"pr_count": data.get('pr_count'), "date": date})

        # Update the matrix data in the database
        cursor.execute("""
            UPDATE vendor_rating_matrix
            SET matrcis = %s
            WHERE partner_detail_id = %s AND tenant = %s;
        """, (json.dumps(matrix_data), partner_detail_id, tenant))

    except mysql.connector.Error as err:
        print("Error: {}".format(err))
        raise

def insert_vendor_rating_matrix(cursor, partner_detail_id, created_on, data, tenant):
    try:
        matrix_data = {
            "vendor_rating": [{"rating": data.get('rating'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
            "fill_rating_qty_level": [{"fill_rate_qty_level": data.get('po_fill_rate_qty_level'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
            "fill_rating_ucode_level": [{"fill_rate_ucode_level": data.get('po_fill_rate_ucode_level'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
            "on_time_delivery_rating": [{"on_time_delivery": data.get('on_time_delivery_rate'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
            "median_lead_time_rating": [{"median_lead_time": data.get('median_lead_time'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
            "perfect_order_rating": [{"perfect_order_rate": data.get('perfect_order_rate'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
            "damaged_rating": [{"damaged": data.get('damaged_items_received'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
            "near_expiry_rating": [{"near_expiry": data.get('expired_near_expiry_items_received'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
            "pr_initiated_rating": [{"pr_count": data.get('pr_count'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}]
        }

        cursor.execute("""
            INSERT INTO vendor_rating_matrix (partner_detail_id, matrcis, tenant)
            VALUES (%s, %s, %s);
        """, (partner_detail_id, json.dumps(matrix_data), tenant))

    except mysql.connector.Error as err:
        print("Error: {}".format(err))
        raise

def insert_vendor_rating_data(data):
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config.yml'))
    connection = create_connection(config_path)
    if connection is None:
        raise Exception("Failed to connect to the database")

    try:
        cursor = connection.cursor()

        for record in data:
            partner_detail_id = record.get('partner_detail_id')
            tenant = record.get('thea_id')

            # Check if record exists in vendor_rating
            cursor.execute("SELECT * FROM vendor_rating WHERE partner_detail_id = %s AND thea_id = %s", (partner_detail_id, tenant))
            existing_record = cursor.fetchone()

            if existing_record:
                # Update existing vendor_rating record
                cursor.execute("""
                    UPDATE vendor_rating
                    SET vendor_name = %s,
                        rating = %s,
                        po_fill_rate_qty_level = %s,
                        po_fill_rate_ucode_level = %s,
                        on_time_delivery_rate = %s,
                        median_lead_time = %s,
                        perfect_order_rate = %s,
                        damaged_items_received = %s,
                        expired_near_expiry_items_received = %s,
                        pr_count = %s,
                        created_on = NOW()
                    WHERE partner_detail_id = %s AND thea_id = %s;
                """, (
                    record.get('vendor_name'), record.get('rating'), record.get('po_fill_rate_qty_level'),
                    record.get('po_fill_rate_ucode_level'), record.get('on_time_delivery_rate'), record.get('median_lead_time'),
                    record.get('perfect_order_rate'), record.get('damaged_items_received'),
                    record.get('expired_near_expiry_items_received'), record.get('pr_count'),
                    partner_detail_id, tenant
                ))

                cursor.execute("SELECT created_on FROM vendor_rating WHERE partner_detail_id = %s AND thea_id = %s", (partner_detail_id, tenant))
                created_on = cursor.fetchone()[0]

                # Update existing vendor_rating_matrix record
                update_vendor_rating_matrix(cursor, partner_detail_id, created_on, record, tenant)

            else:
                # Insert new vendor_rating record
                cursor.execute("""
                    INSERT INTO vendor_rating (
                        vendor_name, partner_detail_id, thea_id, rating, po_fill_rate_qty_level,
                        po_fill_rate_ucode_level, on_time_delivery_rate, median_lead_time, perfect_order_rate,
                        damaged_items_received, expired_near_expiry_items_received, pr_count, created_on
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());
                """, (
                    record.get('vendor_name'), record.get('partner_detail_id'), record.get('thea_id'),
                    record.get('rating'), record.get('po_fill_rate_qty_level'), record.get('po_fill_rate_ucode_level'),
                    record.get('on_time_delivery_rate'), record.get('median_lead_time'), record.get('perfect_order_rate'),
                    record.get('damaged_items_received'), record.get('expired_near_expiry_items_received'), record.get('pr_count')
                ))

                cursor.execute("SELECT created_on FROM vendor_rating WHERE partner_detail_id = %s AND thea_id = %s", (partner_detail_id, tenant))
                created_on = cursor.fetchone()[0]

                # Insert into vendor_rating_matrix
                insert_vendor_rating_matrix(cursor, partner_detail_id, created_on, record, tenant)

        connection.commit()
        return "Data inserted/updated successfully"
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        close_connection(connection)


