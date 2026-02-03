import json
import pandas as pd
from datetime import datetime, timedelta
import os
import traceback
import requests
import concurrent.futures
import time
# Placeholder for DB connection - User can swap with mysql.connector or pymysql
import mysql.connector 
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==========================================
# CONFIGURATION & CREDENTIALS
# ==========================================
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME', 'qfinance')
}

BASE_URL = "https://mum-objectstore.e2enetworks.net/production-finqy/"
# OUTPUT_FILE: Relative path
OUTPUT_FILE = "processed_trade_lines.xlsx"
MAX_WORKERS = 20  # Number of parallel threads

# Target Headers (31 Columns)
TARGET_HEADERS = [
    'pan', 'fiName', 'creditLineType', 'totalSanctionedAmount', 'currentOutstanding', 
    'status', 'paidPrincipalAmount', 'EMI', 'totalTenure', 'pendingTenure', 
    'startDate', 'Balance', 'lastPaymentDate', 'lastPaymentAmount', 
    'accountPastDueAmount', 'totalDelinquencies', 'delinquencies', 
    'delinquencies30Days', 'delinquencies60Days', 'delinquencies90Days', 
    'Recent_Missed_30DPD', 'Recent_Missed_60DPD', 'Recent_Missed_90DPD', 
    'Enq_30Days', 'Enq_60Days', 'Enq_90Days', 'Enq_1Year', 
    'currentDpd', 'settledLast30Days', 'settledLast60Days', 'settledLast90Days'
]

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def clean_money(val):
    if val is None: return 0
    s_val = str(val).replace('*', '').replace(',', '').strip()
    try:
        if s_val == '' or s_val.lower() == 'null': return 0
        return float(s_val)
    except:
        return 0

def clean_str(val):
    if not val: return None
    return str(val).replace('*', '').strip()

def calculate_enquiries(enquiries_list, days):
    if not enquiries_list:
        return 0
    count = 0
    now = datetime.now()
    cutoff_date = now - timedelta(days=days)
    
    for enq in enquiries_list:
        try:
            enq_date_str = enq.get('date')
            if enq_date_str:
                enq_date = datetime.strptime(enq_date_str, "%Y-%m-%d")
                if enq_date >= cutoff_date:
                    count += 1
        except Exception:
            continue
    return count

def get_pending_tenure(total_tenure, open_date_str):
    try:
        clean_tenure = str(total_tenure).replace('*', '').strip()
        if not clean_tenure or clean_tenure.lower() == 'null':
            return 0
        
        total_months = int(float(clean_tenure)) 
        
        if not open_date_str:
            return 0
            
        open_date = datetime.strptime(open_date_str, "%Y-%m-%d")
        now = datetime.now()
        
        months_passed = (now.year - open_date.year) * 12 + (now.month - open_date.month)
        pending = total_months - months_passed
        return max(0, pending) 
    except Exception:
        return 0

def get_delinquency_buckets(payment_history):
    stats = {
        'totalDelinquencies': 0,
        'delinquencies': [],
        'delinq30': 0, 'delinq60': 0, 'delinq90': 0,
        'recent30': 0, 'recent60': 0, 'recent90': 0
    }
    
    if not payment_history:
        return stats
        
    parsed_history = []
    for rec in payment_history:
        try:
            m_str = rec.get('month')
            dt = datetime.strptime(m_str, '%m-%y')
            parsed_history.append({'dt': dt, 'status': str(rec.get('status', '')), 'month_str': m_str})
        except:
            continue
            
    parsed_history.sort(key=lambda x: x['dt'], reverse=True)
    
    now = datetime.now()
    three_months_ago = now - timedelta(days=90)

    for i, rec in enumerate(parsed_history):
        status_raw = rec['status'].replace('*', '').upper()
        
        dpd_val = 0
        is_delinquent = False
        
        if status_raw.isdigit():
            dpd_val = int(status_raw)
            if dpd_val > 0:
                is_delinquent = True
        elif status_raw in ["STD", "STANDARD", "CURRENT", "0", ""]:
             dpd_val = 0
        else:
            is_delinquent = True
            if status_raw in ['SUB', 'DBT', 'LSS']: dpd_val = 90
            elif status_raw.startswith('SMA'): dpd_val = 30
            else: dpd_val = 1 
        
        if is_delinquent:
            stats['totalDelinquencies'] += 1
            stats['delinquencies'].append(rec['month_str'])
            
            if dpd_val >= 30: stats['delinq30'] += 1
            if dpd_val >= 60: stats['delinq60'] += 1
            if dpd_val >= 90: stats['delinq90'] += 1
            
            if rec['dt'] >= three_months_ago:
                if dpd_val >= 30: stats['recent30'] += 1
                if dpd_val >= 60: stats['recent60'] += 1
                if dpd_val >= 90: stats['recent90'] += 1

    stats['delinquencies'] = ",".join(stats['delinquencies'])
    return stats

# ==========================================
# CORE PROCESSING LOGIC (Single JSON Record)
# ==========================================
def process_single_record(data_obj, pan_from_db=None):
    rows = []
    try:
        data = data_obj.get('data', {})
        if not isinstance(data, dict): return []

        report_data = data.get('reportData', {})
        report_summary = report_data.get('reportSummary', {})
        personal_details = report_summary.get('personalDetails', {})
        credit_analysis = report_data.get('creditAnalysis', {})
        enquiries_section = credit_analysis.get('enquiries', {})
        
        pan = pan_from_db if pan_from_db else personal_details.get('pan')

        # ENQUIRIES
        raw_enqs = []
        ce_section = credit_analysis.get('enquiries', {})
        if isinstance(ce_section, dict):
            raw_enqs.extend(ce_section.get('recent', []))
            raw_enqs.extend(ce_section.get('all', []))
            raw_enqs.extend(ce_section.get('previous', []))

        re_section = report_summary.get('enquiries', {})
        if isinstance(re_section, dict):
             raw_enqs.extend(re_section.get('recent', []))
             raw_enqs.extend(re_section.get('all', []))

        unique_enqs_map = {}
        for enq in raw_enqs:
            if not isinstance(enq, dict): continue
            date_str = enq.get('date')
            lender = enq.get('lender') or enq.get('institution') or enq.get('InstitutionName') or 'Unknown'
            if date_str:
                key = (date_str, lender)
                if key not in unique_enqs_map:
                    unique_enqs_map[key] = enq
        
        enq_list = list(unique_enqs_map.values())
        enq_30 = calculate_enquiries(enq_list, 30)
        enq_60 = calculate_enquiries(enq_list, 60)
        enq_90 = calculate_enquiries(enq_list, 90)
        enq_365 = calculate_enquiries(enq_list, 365)

        # ACCOUNTS
        all_accounts = []
        all_accounts.extend(credit_analysis.get('creditCards', []))
        loans_data = credit_analysis.get('loans', {})
        if isinstance(loans_data, dict):
            for key, val in loans_data.items():
                if isinstance(val, list): all_accounts.extend(val)
        elif isinstance(loans_data, list):
             all_accounts.extend(loans_data)
        
        others = credit_analysis.get('others', {})
        if isinstance(others, dict):
            all_accounts.extend(others.get('overdraft', []))
        
        if not all_accounts:
             row = {header: None for header in TARGET_HEADERS}
             row['pan'] = pan
             row['Enq_30Days'] = enq_30
             row['Enq_60Days'] = enq_60
             row['Enq_90Days'] = enq_90
             row['Enq_1Year'] = enq_365
             for k in ['totalDelinquencies', 'delinquencies30Days', 'delinquencies60Days', 'delinquencies90Days', 
                       'Recent_Missed_30DPD', 'Recent_Missed_60DPD', 'Recent_Missed_90DPD',
                       'settledLast30Days', 'settledLast60Days', 'settledLast90Days', 'currentDpd']:
                 row[k] = 0
             rows.append(row)

        for account in all_accounts:
            if not isinstance(account, dict): continue

            delinq_stats = get_delinquency_buckets(account.get('paymentHistory', []))
            
            total_tenure_raw = account.get('repaymentTenure')
            open_date_raw = account.get('accountOpenDate')
            pending_tenure = get_pending_tenure(total_tenure_raw, open_date_raw)
            
            sanctioned_amt = clean_money(account.get('sanctionedAmount') or account.get('totalSanctionAmt'))
            outstanding_amt = clean_money(account.get('outstanding') or account.get('totalBalance'))
            
            paid_principal = 0 
            if 'paidPrincipal' in account:
                 paid_principal = clean_money(account.get('paidPrincipal'))
            else:
                 paid_principal = max(0, sanctioned_amt - outstanding_amt)
            
            status_raw = clean_str(account.get('accountStatus'))
            close_date_raw = account.get('accountCloseDate')
            
            settled30, settled60, settled90 = 0, 0, 0
            if status_raw and 'SETTLED' in status_raw.upper():
                if close_date_raw:
                    try:
                        c_date = datetime.strptime(close_date_raw, "%Y-%m-%d")
                        days_diff = (datetime.now() - c_date).days
                        if days_diff <= 30: settled30 = 1
                        if days_diff <= 60: settled60 = 1
                        if days_diff <= 90: settled90 = 1
                    except: pass

            row = {
                'pan': pan,
                'fiName': account.get('provider'),
                'creditLineType': account.get('accountType') or account.get('product'),
                'totalSanctionedAmount': sanctioned_amt,
                'currentOutstanding': outstanding_amt,
                'status': status_raw,
                'paidPrincipalAmount': paid_principal,
                'EMI': clean_money(account.get('emi')),
                'totalTenure': clean_str(total_tenure_raw),
                'pendingTenure': pending_tenure,
                'startDate': clean_str(open_date_raw),
                'Balance': outstanding_amt,
                'lastPaymentDate': clean_str(account.get('lastPaymentDate')),
                'lastPaymentAmount': clean_money(account.get('lastPaymentAmount')),
                'accountPastDueAmount': clean_money(account.get('accountPastDueAmount')),
                'totalDelinquencies': delinq_stats['totalDelinquencies'],
                'delinquencies': delinq_stats['delinquencies'],
                'delinquencies30Days': delinq_stats['delinq30'],
                'delinquencies60Days': delinq_stats['delinq60'],
                'delinquencies90Days': delinq_stats['delinq90'],
                'Recent_Missed_30DPD': delinq_stats['recent30'],
                'Recent_Missed_60DPD': delinq_stats['recent60'],
                'Recent_Missed_90DPD': delinq_stats['recent90'],
                'Enq_30Days': enq_30,
                'Enq_60Days': enq_60,
                'Enq_90Days': enq_90,
                'Enq_1Year': enq_365,
                'currentDpd': clean_money(account.get('accountPastDueAmount')),
                'settledLast30Days': settled30,
                'settledLast60Days': settled60,
                'settledLast90Days': settled90
            }
            rows.append(row)
            
    except Exception:
        # traceback.print_exc()
        pass
    return rows

def fetch_and_process_task(item):
    """
    Worker function to be executed in parallel.
    item is a tuple: (pan, json_filename)
    """
    pan, json_filename = item
    full_url = BASE_URL + json_filename
    
    try:
        resp = requests.get(full_url, timeout=30)
        if resp.status_code == 200:
            json_data = resp.json()
            return process_single_record(json_data, pan_from_db=pan)
        else:
            print(f"[ERROR] Failed download for {pan}: {resp.status_code}")
            return []
    except Exception as e:
        print(f"[ERROR] Exception for {pan}: {e}")
        return []

# ==========================================
# MAIN EXECUTION ROUTINE (Refactored for UI)
# ==========================================
def run_processor(max_workers=20, specific_pans=None, progress_callback=None):
    """
    Executes the processing logic.
    :param max_workers: Int, number of threads.
    :param specific_pans: List[str], optional list of PANs to filter by.
    :param progress_callback: Function(current, total, message) for UI updates.
    :return: DataFrame (processed data) or None if error/empty.
    """
    if progress_callback: progress_callback(0, 0, "Initializing Database Connection...")
    print("Starting process...")
    all_final_rows = []

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        query = ""
        # 1. BUILD QUERY
        if specific_pans and len(specific_pans) > 0:
            msg = f"Fetching records for {len(specific_pans)} specific PANs..."
            print(msg)
            if progress_callback: progress_callback(0, 0, msg)
            
            # Sanitization for SQL IN clause
            # (In production, consider cleaner param binding, but list injection is okay for this scope)
            safe_pans = [p.replace("'", "") for p in specific_pans]
            pan_list_str = "', '".join(safe_pans)
            
            query = f"SELECT pancardNumber, recommendationJsonFile FROM qfinance.q_report WHERE pancardNumber IN ('{pan_list_str}') ORDER BY createdAt DESC"
            
        else:
            msg = "Fetching ALL records from database..."
            print(msg)
            if progress_callback: progress_callback(0, 0, msg)
            
            query = "SELECT pancardNumber, recommendationJsonFile FROM qfinance.q_report ORDER BY createdAt DESC"
        
        cursor.execute(query)
        records = cursor.fetchall()
        print(f"Found {len(records)} total records to process.")
        
        # 2. IDENTIFY UNIQUE TASKS (Main Thread)
        unique_tasks = []
        seen_pans = set()
        
        if progress_callback: progress_callback(0, len(records), "Filtering Duplicates (Latest Wins)...")
        print("Identifying unique/latest reports assigned to tasks...")
        for idx, (pan, json_filename) in enumerate(records):
            if not json_filename or str(json_filename).lower() == 'null':
                continue
            
            if pan in seen_pans:
                continue
            seen_pans.add(pan)
            
            unique_tasks.append((pan, json_filename))
        
        total_tasks = len(unique_tasks)
        print(f"Total Unique Valid Tasks to Process: {total_tasks}")
        
        if total_tasks == 0:
            if progress_callback: progress_callback(0, 0, "No records found matching criteria.")
            conn.close()
            return None

        if progress_callback: progress_callback(0, total_tasks, f"Starting Parallel Processing for {total_tasks} Tasks...")
        
        # 3. PARALLEL EXECUTION
        start_time = time.time()
        
        print(f"Starting {max_workers} parallel threads...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_pan = {executor.submit(fetch_and_process_task, task): task[0] for task in unique_tasks}
            
            for i, future in enumerate(concurrent.futures.as_completed(future_to_pan)):
                pan = future_to_pan[future]
                try:
                    rows = future.result()
                    all_final_rows.extend(rows)
                    
                    # Update Progress
                    if progress_callback:
                        msg = f"Processed {i+1}/{total_tasks}: {pan}"
                        progress_callback(i+1, total_tasks, msg)
                        
                    if (i + 1) % 50 == 0:
                        print(f"Processed {i + 1}/{total_tasks} records...")
                        
                except Exception as exc:
                    print(f"Task for {pan} generated an exception: {exc}")
                    
        elapsed_time = time.time() - start_time
        print(f"\nProcessing completed in {elapsed_time:.2f} seconds.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        if progress_callback: progress_callback(0, 0, f"Error: {e}")
        return None

    if all_final_rows:
        if progress_callback: progress_callback(total_tasks, total_tasks, "Generating Excel File...")
        df = pd.DataFrame(all_final_rows)
        df = df.reindex(columns=TARGET_HEADERS)
        try:
            df.to_excel(OUTPUT_FILE, index=False)
            print(f"\nSUCCESS! Wrote {len(df)} rows to {OUTPUT_FILE}")
            return df
        except Exception as e:
            print(f"Error writing Excel: {e}")
            return df
    else:
        print("\nNo data processed.")
        return None

if __name__ == "__main__":
    # Standard CLI execution
    run_processor(max_workers=MAX_WORKERS)
