import os
import sys
from flask import Flask, request, jsonify
from service.vendor_rating_service import insert_vendor_rating_data
from service.vendor_summary_service import get_summary

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
