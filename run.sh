#!/bin/bash

echo "=========================================="
echo "   TRADELINE PROCESSOR - LINUX LAUNCHER"
echo "=========================================="
echo ""

# 1. Check for Python
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] python3 could not be found."
    exit 1
fi

# 2. Install Dependencies
echo "[1] Installing Dependencies..."
pip3 install -r requirements.txt

# 3. Run Application
echo "[2] Launching App..."
echo "To keep this running in background, use: nohup ./run.sh &"
echo "App running at http://0.0.0.0:8501"
echo ""

streamlit run app.py --server.port 8501 --server.address 0.0.0.0
