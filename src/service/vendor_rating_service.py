import json
import os
import sys
import mysql.connector

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sql')))
from db_connection import create_connection, close_connection

def update_vendor_rating_matrix(cursor, partner_detail_id, created_on, data):
    cursor.execute("SELECT matrcis FROM vendor_rating_matrix WHERE partner_detail_id = %s", (partner_detail_id,))
    matrix_row = cursor.fetchone()

    matrix_data = json.loads(matrix_row[0]) if matrix_row else {
        "vendor_rating": [],
        "fill_rating": [],
        "delivery_lead_time_rating": [],
        "damaged_rating": [],
        "near_expiry_rating": [],
        "pr_initiated_rating": []
    }

    date = created_on.strftime('%Y-%m-%d %H:%M:%S')

    matrix_data['vendor_rating'].append({"rating": data.get('rating'), "date": date})
    matrix_data['fill_rating'].append({"fill_rate": data.get('fill_rate'), "date": date})
    matrix_data['delivery_lead_time_rating'].append({"delivery_lead_time": data.get('delivery_lead_time'), "date": date})
    matrix_data['damaged_rating'].append({"damaged": data.get('damaged'), "date": date})
    matrix_data['near_expiry_rating'].append({"near_expiry": data.get('near_expiry_item'), "date": date})
    matrix_data['pr_initiated_rating'].append({"pr_initiated": data.get('pr_initiated'), "date": date})

    cursor.execute("""
        UPDATE vendor_rating_matrix
        SET matrcis = %s
        WHERE partner_detail_id = %s;
    """, (json.dumps(matrix_data), partner_detail_id))


def insert_vendor_rating_data(data):
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config.yml'))
    connection = create_connection(config_path)
    if connection is None:
        raise Exception("Failed to connect to the database")

    try:
        cursor = connection.cursor()

        for record in data:
            partner_detail_id = record.get('partner_detail_id')
            tenant = record.get('tenant')

            # Check if record exists in vendor_rating
            cursor.execute("SELECT * FROM vendor_rating WHERE partner_detail_id = %s AND tenant = %s", (partner_detail_id, tenant))
            existing_record = cursor.fetchone()

            if existing_record:
                cursor.execute("""
                    UPDATE vendor_rating
                    SET partner_name = %s,
                        rating = %s,
                        fill_rate = %s,
                        delivery_lead_time = %s,
                        damaged = %s,
                        near_expiry_item = %s,
                        pr_initiated = %s,
                        created_on = NOW()
                    WHERE partner_detail_id = %s AND tenant = %s;
                """, (
                    record.get('partner_name'), record.get('rating'), record.get('fill_rate'),
                    record.get('delivery_lead_time'), record.get('damaged'), record.get('near_expiry_item'),
                    record.get('pr_initiated'), partner_detail_id, tenant
                ))

                cursor.execute("SELECT created_on FROM vendor_rating WHERE partner_detail_id = %s AND tenant = %s", (partner_detail_id, tenant))
                created_on = cursor.fetchone()[0]

                update_vendor_rating_matrix(cursor, partner_detail_id, created_on, record)

            else:
                cursor.execute("""
                    INSERT INTO vendor_rating (
                        partner_name, partner_detail_id, tenant, rating, fill_rate,
                        delivery_lead_time, damaged, near_expiry_item, pr_initiated, created_on
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());
                """, (
                    record.get('partner_name'), record.get('partner_detail_id'), record.get('tenant'),
                    record.get('rating'), record.get('fill_rate'), record.get('delivery_lead_time'),
                    record.get('damaged'), record.get('near_expiry_item'), record.get('pr_initiated')
                ))

                cursor.execute("SELECT created_on FROM vendor_rating WHERE partner_detail_id = %s AND tenant = %s", (partner_detail_id, tenant))
                created_on = cursor.fetchone()[0]

                matrix_data = {
                    "vendor_rating": [{"rating": record.get('rating'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
                    "fill_rating": [{"fill_rate": record.get('fill_rate'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
                    "delivery_lead_time_rating": [{"delivery_lead_time": record.get('delivery_lead_time'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
                    "damaged_rating": [{"damaged": record.get('damaged'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
                    "near_expiry_rating": [{"near_expiry": record.get('near_expiry_item'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}],
                    "pr_initiated_rating": [{"pr_initiated": record.get('pr_initiated'), "date": created_on.strftime('%Y-%m-%d %H:%M:%S')}]
                }

                cursor.execute("""
                    INSERT INTO vendor_rating_matrix (partner_detail_id, matrcis)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE matrcis = VALUES(matrcis);
                """, (partner_detail_id, json.dumps(matrix_data)))

        connection.commit()
        return "Data inserted/updated successfully"
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        close_connection(connection)