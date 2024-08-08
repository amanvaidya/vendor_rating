import os
import sys
import mysql.connector

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sql')))
from db_connection import create_connection, close_connection


def get_vendor_ratings(page, page_size):
    connection = None
    page_size = int(page_size)
    page = int(page)
    try:
        # Calculate the offset
        offset = page * page_size

        # SQL query to fetch pageable data
        query = """
            SELECT id, vendor_name, partner_detail_id, thea_id, rating, po_fill_rate_qty_level, 
                   po_fill_rate_ucode_level, on_time_delivery_rate, median_lead_time, perfect_order_rate, 
                   damaged_items_received, expired_near_expiry_items_received, pr_count, created_on
            FROM vendor_rating
            ORDER BY created_on DESC
            LIMIT %s OFFSET %s;
        """

        # Connect to the database
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config.yml'))
        connection = create_connection(config_path)
        cursor = connection.cursor()

        # Execute the query with page_size and offset
        cursor.execute(query, (page_size, offset))
        # cursor.execute(query, (page_size, offset))
        results = cursor.fetchall()

        # Fetch total count for pagination
        cursor.execute("SELECT COUNT(*) FROM vendor_rating;")
        total_count = cursor.fetchone()[0]

        # Prepare the response
        data = []
        for row in results:
            data.append({
                'id': row[0],
                'vendor_name': row[1],
                'partner_detail_id': row[2],
                'thea_id': row[3],
                'rating': row[4],
                'po_fill_rate_qty_level': row[5],
                'po_fill_rate_ucode_level': row[6],
                'on_time_delivery_rate': row[7],
                'median_lead_time': row[8],
                'perfect_order_rate': row[9],
                'damaged_items_received': row[10],
                'expired_near_expiry_items_received': row[11],
                'pr_count': row[12],
                'created_on': row[13].strftime('%Y-%m-%d %H:%M:%S')
            })

        # Pagination info
        response = {
            'data': data,
            'page': page,
            'page_size': page_size,
            'total_count': total_count,
            'total_pages': (total_count + 10 - 1) // 10
        }

        return response

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        raise
    finally:
        if connection:
            close_connection(connection)
        # close_connection(connection)
