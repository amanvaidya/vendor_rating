import plotly.graph_objs as go
import plotly.io as pio
from datetime import datetime
import json
import os
import sys
import mysql.connector

# Adding the path for database connection utilities
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sql')))
from db_connection import create_connection, close_connection


# Function to normalize data
def normalize(data):
    min_val = min(data)
    max_val = max(data)
    if max_val == min_val:
        return [0] * len(data)
    return [(float(x) - float(min_val)) / (float(max_val) - float(min_val)) for x in data]


def plot(partner_detail_id, tenant):
    # Establish database connection
    connection = create_connection()

    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT matrcis FROM vendor_rating_matrix WHERE partner_detail_id = %s AND tenant = %s",
            (partner_detail_id, tenant)
        )
        result = cursor.fetchone()

        if result is None:
            raise ValueError("No data found for the specified partner_detail_id and tenant.")

        # Parsing JSON data
        data = json.loads(result[0])

        # Extracting data
        vendor_ratings = data["vendor_rating"]
        fill_ratings = data["fill_rating"]
        delivery_lead_time_rating = data["delivery_lead_time_rating"]
        damaged_rating = data["damaged_rating"]
        near_expiry_rating = data["near_expiry_rating"]

        # Sorting data by date
        vendor_ratings.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))
        fill_ratings.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))
        delivery_lead_time_rating.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))
        damaged_rating.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))
        near_expiry_rating.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))

        # Extracting dates and values
        vendor_dates = [datetime.strptime(entry["date"], "%Y-%m-%d %H:%M:%S") for entry in vendor_ratings]
        vendor_values = [entry["rating"] for entry in vendor_ratings]

        fill_dates = [datetime.strptime(entry["date"], "%Y-%m-%d %H:%M:%S") for entry in fill_ratings]
        fill_values = [entry["fill_rate"] for entry in fill_ratings]

        delivery_lead_dates = [datetime.strptime(entry["date"], "%Y-%m-%d %H:%M:%S") for entry in delivery_lead_time_rating]
        delivery_lead_values = [entry["delivery_lead_time"] for entry in delivery_lead_time_rating]

        damaged_dates = [datetime.strptime(entry["date"], "%Y-%m-%d %H:%M:%S") for entry in damaged_rating]
        damaged_values = [entry["damaged"] for entry in damaged_rating]

        near_expiry_dates = [datetime.strptime(entry["date"], "%Y-%m-%d %H:%M:%S") for entry in near_expiry_rating]
        near_expiry_values = [entry["near_expiry"] for entry in near_expiry_rating]

        # Normalizing the data
        normalized_vendor_values = normalize(vendor_values)
        normalized_fill_values = normalize(fill_values)
        normalized_delivery_lead_values = normalize(delivery_lead_values)
        normalized_damaged_values = normalize(damaged_values)
        normalized_near_expiry_values = normalize(near_expiry_values)

        # Plotting with Plotly
        fig = go.Figure()

        fig.add_trace(go.Scatter(x=vendor_dates, y=normalized_vendor_values, mode='lines+markers', name='Normalized Vendor Rating'))
        fig.add_trace(go.Scatter(x=fill_dates, y=normalized_fill_values, mode='lines+markers', name='Normalized Fill Rating'))
        fig.add_trace(go.Scatter(x=delivery_lead_dates, y=normalized_delivery_lead_values, mode='lines+markers', name='Normalized Delivery Lead Time Rating'))
        fig.add_trace(go.Scatter(x=damaged_dates, y=normalized_damaged_values, mode='lines+markers', name='Normalized Damaged Rating'))
        fig.add_trace(go.Scatter(x=near_expiry_dates, y=normalized_near_expiry_values, mode='lines+markers', name='Normalized Near Expiry Rating'))

        # Formatting
        fig.update_layout(
            title='Normalized Vendor Metrics Over Time',
            xaxis_title='Date',
            yaxis_title='Normalized Rating',
            xaxis_tickformat='%Y-%m-%d',
            xaxis_tickangle=-45,
            template='plotly_white',
            legend_title="Metrics"
        )

        # Convert to HTML
        plot_html = pio.to_html(fig, full_html=False)
        return plot_html

    except Exception as e:
        connection.rollback()
        raise e
    finally:
        close_connection(connection)