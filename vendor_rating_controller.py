import os
import sys
from flask import Flask, request, jsonify, render_template_string
from service.vendor_rating_service import insert_vendor_rating_data
from service.vendor_summary_service import get_summary
from service.reader.csv_reader_service import trigger_cron
from service.vendor_service import get_vendor_ratings

# Add the path to the service directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'service')))

app = Flask(__name__)

@app.route('/vendor_ratings', methods=['POST'])
def vendor_ratings():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    try:
        insert_vendor_rating_data(data)
        return jsonify({'message': 'Data inserted/updated successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/summary', methods=['GET'])
def vendor_summary():
    # Validate 'partner_detail_id'
    # if not pdi:
    #     return jsonify({'error': 'No partner_detail_id provided'}), 400
    pdi = request.args.get('partner_detail_id')
    tenant = request.args.get('tenant')
    try:
        # Call get_summary and receive the HTML content
        html_content = get_summary(pdi, tenant)

        # Return the HTML content as a response
        return html_content, 200, {'Content-Type': 'text/html'}
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/data', methods=['POST'])
def start_cron():
    trigger_cron()


@app.route('/get-data', methods=['GET'])
def get_data():
    page = int(request.args.get('page', 0))
    page_size = request.args.get('page_size')  # Set the page size

    # Fetch paginated vendor ratings
    vendor_ratings = get_vendor_ratings(page, page_size)

    # HTML content as a string
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Vendor Ratings</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 20px;
                background-color: #f4f4f4;
            }
            h1 {
                text-align: center;
                color: #333;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
                background-color: #fff;
                box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
            }
            th, td {
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }
            th {
                background-color: #4CAF50;
                color: white;
            }
            tr:hover {
                background-color: #f1f1f1;
            }
            .pagination {
                display: flex;
                justify-content: center;
                margin: 20px 0;
            }
            .pagination a {
                color: #333;
                padding: 8px 16px;
                text-decoration: none;
                border: 1px solid #ddd;
                margin: 0 4px;
                border-radius: 4px;
            }
            .pagination a:hover {
                background-color: #4CAF50;
                color: white;
            }
            @media (max-width: 768px) {
                table, th, td {
                    display: block;
                    width: 100%;
                }
                th, td {
                    text-align: right;
                }
                th {
                    background-color: transparent;
                    color: #333;
                    border-bottom: none;
                }
                th:before {
                    content: attr(data-label);
                    font-weight: bold;
                    float: left;
                    color: #4CAF50;
                }
                td:before {
                    content: attr(data-label);
                    font-weight: bold;
                    float: left;
                    color: #333;
                }
                td, th {
                    padding: 8px;
                }
            }
        </style>
    </head>
    <body>
        <h1>Vendor Ratings</h1>

        <table border="1">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Vendor Name</th>
                    <th>Partner Detail ID</th>
                    <th>Tenant</th>
                    <th>Rating</th>
                    <th>PO Fill Rate Qty Level</th>
                    <th>PO Fill Rate Ucode Level</th>
                    <th>On Time Delivery Rate</th>
                    <th>Perfect Order Rate</th>
                    <th>Damaged Items Received</th>
                    <th>Expired Near Expiry Items Received</th>
                    <th>PR Count</th>
                    <th>Summary</th>
                </tr>
            </thead>
            <tbody>
                {% for vendor in vendor_ratings.data %}
                <tr>
                    <td>{{ vendor.id }}</td>
                    <td>{{ vendor.vendor_name }}</td>
                    <td>{{ vendor.partner_detail_id }}</td>
                    <td>{{ vendor.thea_id }}</td>
                    <td>{{ vendor.rating }}</td>
                    <td>{{ vendor.po_fill_rate_qty_level }}</td>
                    <td>{{ vendor.po_fill_rate_ucode_level }}</td>
                    <td>{{ vendor.on_time_delivery_rate }}</td>
                    <td>{{ vendor.perfect_order_rate }}</td>
                    <td>{{ vendor.damaged_items_received }}</td>
                    <td>{{ vendor.expired_near_expiry_items_received }}</td>
                    <td>{{ vendor.pr_count }}</td>
                    <td><a href="/summary?partner_detail_id={{ vendor.partner_detail_id }}&tenant={{ vendor.thea_id }}" target="_blank">View Summary</a></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>

        <div>
            <p>Page {{ vendor_ratings.page + 1 }} of {{ vendor_ratings.total_pages }}</p>
            {% if vendor_ratings.page > 0 %}
                <a href="get-data?page={{ vendor_ratings.page - 1 }}&page_size=5">Previous</a>
            {% endif %}
            {% if vendor_ratings.page < vendor_ratings.total_pages - 1 %}
                <a href="get-data?page={{ vendor_ratings.page + 1 }}&page_size=5">Next</a>
            {% endif %}
        </div>
    </body>
    </html>
    """

    return render_template_string(html_content, vendor_ratings=vendor_ratings)


