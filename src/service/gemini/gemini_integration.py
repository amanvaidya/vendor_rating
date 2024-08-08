import google.generativeai as genai
import markdown
from service.graph_plot import plot
from service.floating_graph_plot import plot_data
from flask import url_for, Flask

app = Flask(__name__)

def gemini_integration(po_fill_rate_qty_level, fill_rate_ucode_level, on_time_delivery_rate, perfect_order_rate,  near_expiry_expired_items, damaged_items, purchase_returns_initiated, partner_detail_id, tenant):
    genai.configure(api_key='AIzaSyBNiA5tPaiwLqz72eYLlj_83TULNJkDYeU')
    model = genai.GenerativeModel('gemini-1.5-flash')
    # plot()

    # Create the prompt string
    prompt = (
        f"Create a portfolio in low level English of the vendor to understand based on these facts: "
        f"Fill Rate % at Qty Level is {po_fill_rate_qty_level}. "
        f"Fill Rate % at UCode Level is {fill_rate_ucode_level}. "
        f"Perfect Order Rate is {perfect_order_rate}. "
        f"On Time Delivery Rate is {on_time_delivery_rate}. "
        f"Near Expiry / Expired Items Received is {near_expiry_expired_items}. "
        f"Damaged Items Received is {damaged_items}. "
        f"Purchase Returns Initiated is {purchase_returns_initiated}."
    )

    response = model.generate_content(prompt)

    # Generate the image URL inside an active request context
    with app.app_context():
        image_url = plot(partner_detail_id, tenant)
        floating_image_url = plot_data(partner_detail_id, tenant)
    html_response = f"""
    <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Vendor Portfolio</title>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                .container {{ width: 80%; margin: 0 auto; padding: 20px; }}
                h1 {{ text-align: center; }}
                .content {{ margin-top: 20px; display: flex; justify-content: space-between; }}
                .content-text, .content-summary {{ width: 48%; }}
                .foot-container {{ margin-top: 20px; text-align: center; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Vendor Portfolio</h1>
                <div class="content">
                    <div class="content-text">
                        {markdown.markdown(response.candidates[0].content.parts[0].text)}
                    </div>
                    <div class="content-summary">
                        {image_url}
                    </div>
                </div>
                <div class="foot-container">
                        {floating_image_url}
                </div>
            </div>
        </body>
        </html>
    """

    return html_response