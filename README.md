# 📊 Tradeline Processor

High-performance credit report processing system with a modern Streamlit UI. 
Designed to fetch Experian reports from a MySQL database, deduplicate them (Latest Wins), and export analyzed data to Excel.

## 🚀 Features

*   **Parallel Processing**: Uses multi-threading (`ThreadPoolExecutor`) to download and process 20+ reports/second.
*   **Smart Deduplication**: Automatically identifies and processes only the **latest** report for each PAN (based on `createdAt`).
*   **Regex Filtering**: Paste any list (bullets, emails, messy text) and the app scans for valid PAN patterns (`ABCDE1234F`).
*   **Excel Export**: Generates a strictly formatted `.xlsx` file with 30+ columns of risk analysis (Tenure, Enquiries, Delinquency Buckets).

## 🛠️ Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/waghelakartik/tradeline-processor.git
    cd tradeline-processor
    ```

2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Environment Setup**:
    Create a `.env` file in the root directory with your Database credentials:
    ```ini
    DB_HOST=your_host
    DB_USER=your_user
    DB_PASSWORD=your_password
    DB_NAME=qfinance
    ```

## ⚡ Usage

Run the dashboard locally or on a server:

```bash
streamlit run app.py
```

*   **Local**: Visit `http://localhost:8501`
*   **Network**: Visit `http://YOUR_IP:8501`

## 🔍 How to Filter
In the Sidebar, you can paste specific PAN cards to process.
The input supports **Rich Paste**:
*   ✅ Bulleted lists
*   ✅ Comma-separated values
*   ✅ Text/Emails containing PANs

The system ignores special characters and extracts valid PANs automatically.
