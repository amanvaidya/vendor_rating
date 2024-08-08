import plotly.graph_objs as go
from plotly.subplots import make_subplots
from datetime import datetime
import json
import os
import sys
import mysql.connector
import plotly.io as pio

# Adding the path for database connection utilities
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sql')))
from db_connection import create_connection, close_connection

# Function to fetch data from the database
def fetch_data(partner_detail_id, tenant):
    connection = create_connection()

    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT matrcis 
            FROM vendor_rating_matrix 
            WHERE partner_detail_id = %s AND tenant = %s
            """,
            (partner_detail_id, tenant)
        )
        result = cursor.fetchone()

        if result is None:
            raise ValueError("No data found for the specified partner_detail_id and tenant.")

        # Parsing JSON data
        data = json.loads(result[0])

        # Extracting data
        vendor_ratings = data["vendor_rating"]
        fill_ratings = data["fill_rating_qty_level"]
        delivery_lead_time_rating = data["on_time_delivery_rating"]
        damaged_rating = data["damaged_rating"]
        near_expiry_rating = data["near_expiry_rating"]
        pr_initiated_rating = data["pr_initiated_rating"]

        # Sorting data by date
        vendor_ratings.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))
        fill_ratings.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))
        delivery_lead_time_rating.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))
        damaged_rating.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))
        near_expiry_rating.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))
        pr_initiated_rating.sort(key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"))

        # Extracting dates and values
        dates = [datetime.strptime(entry["date"], "%Y-%m-%d %H:%M:%S") for entry in vendor_ratings]
        vendor_values = [entry["rating"] for entry in vendor_ratings]
        fill_values = [entry["fill_rate_qty_level"] for entry in fill_ratings]
        delivery_lead_values = [entry["on_time_delivery"] for entry in delivery_lead_time_rating]
        damaged_values = [entry["damaged"] for entry in damaged_rating]
        near_expiry_values = [entry["near_expiry"] for entry in near_expiry_rating]
        pr_initiated_values = [entry["pr_count"] for entry in pr_initiated_rating]

        return dates, vendor_values, fill_values, delivery_lead_values, damaged_values, near_expiry_values, pr_initiated_values

    except Exception as e:
        connection.rollback()
        raise e
    finally:
        close_connection(connection)

# Function to plot the data and return as HTML image
def plot_data(partner_detail_id, tenant):
    # Fetch data
    dates, vendor_rating, fill_rating, delivery_lead_time_rating, damaged_rating, near_expiry_rating, pr_initiated_rating = fetch_data(partner_detail_id, tenant)

    # Create subplots
    fig = make_subplots(
        rows=1, cols=6,
        subplot_titles=(
            "Vendor Rating", "Fill Rating", "Delivery Lead Time Rating",
            "Damaged Rating", "Near Expiry Rating", "PR Initiated Rating"
        ),
        shared_xaxes=True,
        shared_yaxes=True
    )

    # Add traces to the subplots
    fig.add_trace(go.Scatter(x=dates, y=vendor_rating, mode='markers+lines', name="Vendor Rating"), row=1, col=1)
    fig.add_trace(go.Scatter(x=dates, y=fill_rating, mode='markers+lines', name="Fill Rating"), row=1, col=2)
    fig.add_trace(go.Scatter(x=dates, y=delivery_lead_time_rating, mode='markers+lines', name="Delivery Lead Time Rating"), row=1, col=3)
    fig.add_trace(go.Scatter(x=dates, y=damaged_rating, mode='markers+lines', name="Damaged Rating"), row=1, col=4)
    fig.add_trace(go.Scatter(x=dates, y=near_expiry_rating, mode='markers+lines', name="Near Expiry Rating"), row=1, col=5)
    fig.add_trace(go.Scatter(x=dates, y=pr_initiated_rating, mode='markers+lines', name="PR Initiated Rating"), row=1, col=6)

    # Update layout
    fig.update_layout(
        title='Vendor Ratings Over Time',
        xaxis_title='Date',
        yaxis_title='Rating',
        showlegend=False,
        height=400
    )

    # Convert the figure to an HTML string
    plot_html = pio.to_html(fig, full_html=False)

    return plot_html

