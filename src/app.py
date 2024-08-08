import sys
import os

# Add the path to the src directory to sys.path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from controller.vendor_rating_controller import app

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)