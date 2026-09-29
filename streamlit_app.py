"""
Streamlit Web Application Root Launcher
=======================================
Launches the interactive Spark MLlib Salary Prediction Web Application.
"""

import os
import sys
import runpy

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Run the primary prediction application
app_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "grafana", "predict_app.py")
runpy.run_path(app_path, run_name="__main__")
