@echo off
echo ==========================================
echo   TRADELINE PROCESSOR - SERVER LAUNCHER
echo ==========================================
echo.
echo [1] Checking for Python...
python --version
if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed or not in PATH.
    pause
    exit /b
)

echo [2] Installing Dependencies...
pip install -r requirements.txt

echo [3] Launching App...
echo.
echo Application will run on port 8501.
echo Access via http://localhost:8501 or http://YOUR_SERVER_IP:8501
echo.
streamlit run app.py --server.port 8501 --server.address 0.0.0.0

pause
