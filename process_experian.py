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

# Target Headers (36 Columns)
TARGET_HEADERS = [
    'pan', 'fiName', 'creditLineType', 'totalSanctionedAmount', 'currentOutstanding', 
    'status', 'SuitFiled', 'SuitFiledStatus', 'WrittenOffFlag', 'WrittenOffAmount',
    'paidPrincipalAmount', 'EMI', 'totalTenure', 'pendingTenure', 
    'startDate', 'Balance', 'lastPaymentDate', 'lastPaymentAmount', 
    'accountPastDueAmount', 'OverdueAmount', 'totalDelinquencies', 'delinquencies', 
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

def clean_nullable_str(val):
    if val is None:
        return None
    s_val = str(val).replace('*', '').strip()
    if s_val == '' or s_val.lower() == 'null':
        return None
    return s_val

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

def get_suit_filed_info(payment_history):
    if not isinstance(payment_history, list) or len(payment_history) == 0:
        return "No", None

    dated_statuses = []
    undated_status = None

    for rec in payment_history:
        if not isinstance(rec, dict):
            continue
        suit_status = clean_nullable_str(rec.get('suitFiledStatus'))
        if not suit_status:
            continue

        month_str = rec.get('month')
        try:
            parsed_month = datetime.strptime(month_str, '%m-%y')
            dated_statuses.append((parsed_month, suit_status))
        except Exception:
            if undated_status is None:
                undated_status = suit_status

    if dated_statuses:
        dated_statuses.sort(key=lambda x: x[0], reverse=True)
        return "Yes", dated_statuses[0][1]

    if undated_status:
        return "Yes", undated_status

    return "No", None

def get_written_off_info(account, status_raw=None):
    written_off_amount = clean_money(account.get('writtenOffAmtTotal'))
    if written_off_amount <= 0:
        fallback_amt = clean_money(account.get('noWriteOff'))
        if fallback_amt > 0:
            written_off_amount = fallback_amt

    status_text = (status_raw or clean_str(account.get('accountStatus')) or '').upper()
    is_written_off = written_off_amount > 0 or ('WRITTEN' in status_text and 'OFF' in status_text)

    return ("Yes" if is_written_off else "No"), written_off_amount

def parse_flexible_date(val):
    if not val:
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")

    s_val = str(val).strip()
    if not s_val or s_val.lower() == 'null':
        return None

    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y%m%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            parsed = datetime.strptime(s_val, fmt)
            if fmt == "%Y-%m":
                parsed = parsed.replace(day=1)
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            continue
    if "T" in s_val:
        return parse_flexible_date(s_val.split("T", 1)[0])
    return s_val

def _build_in_clause(values):
    return ", ".join(["%s"] * len(values))

def get_enquiry_summary_count(summary, *keys):
    if not isinstance(summary, dict):
        return None
    for key in keys:
        if key not in summary:
            continue
        val = summary.get(key)
        try:
            if val is None or str(val).strip() == '':
                continue
            return int(float(str(val).replace(',', '').strip()))
        except Exception:
            continue
    return None

def normalize_api_suit_filed_status(val):
    status = clean_nullable_str(val)
    if not status:
        return None
    if status in {'0', '00', '000', 'N', 'NO'}:
        return None
    return status

def normalize_api_payment_history(payment_history, suit_filed_status=None):
    normalized = []

    for idx, rec in enumerate(payment_history or []):
        if not isinstance(rec, dict):
            continue

        month_str = None
        date_val = rec.get('date') or rec.get('month')
        if date_val:
            date_str = str(date_val).strip()
            for fmt in ("%Y-%m", "%Y-%m-%d", "%m-%y"):
                try:
                    parsed = datetime.strptime(date_str, fmt)
                    month_str = parsed.strftime("%m-%y")
                    break
                except Exception:
                    continue
        if not month_str:
            continue

        days_late = clean_money(rec.get('daysLate'))
        if days_late > 0:
            status = str(int(days_late))
        else:
            status_token = clean_nullable_str(rec.get('status')) or clean_nullable_str(rec.get('assetClassification'))
            status_upper = status_token.upper() if status_token else ''
            if status_upper in {'S', 'STD', 'STANDARD', 'CURRENT', '?', '0'}:
                status = '0'
            else:
                status = status_token or '0'

        entry = {'month': month_str, 'status': status}
        if idx == 0 and suit_filed_status:
            entry['suitFiledStatus'] = suit_filed_status
        normalized.append(entry)

    if suit_filed_status and not normalized:
        normalized.append({
            'month': datetime.now().strftime("%m-%y"),
            'status': '0',
            'suitFiledStatus': suit_filed_status
        })

    return normalized

def normalize_api_enquiries(enquiries):
    normalized = []
    for enq in enquiries or []:
        if not isinstance(enq, dict):
            continue
        date_str = parse_flexible_date(
            enq.get('date') or enq.get('enquiryDate') or enq.get('applicationDate') or enq.get('inquiryDate')
        )
        lender = clean_str(
            enq.get('lender') or enq.get('institution') or enq.get('InstitutionName') or enq.get('memberName') or enq.get('provider')
        )
        if date_str:
            normalized.append({'date': date_str, 'lender': lender or 'Unknown'})
    return normalized

def normalize_api_account_status(status):
    status_text = clean_str(status)
    if not status_text:
        return None
    mapped = {
        'ACTIVE': 'Current Account',
        'CURRENT': 'Current Account',
        'CLOSED': 'Closed Account'
    }
    return mapped.get(status_text.upper(), status_text)

def normalize_positive_tenure(val):
    cleaned = clean_str(val)
    if cleaned is None:
        return None
    try:
        numeric = float(cleaned)
        if numeric <= 0:
            return None
        if numeric.is_integer():
            return str(int(numeric))
        return str(numeric)
    except Exception:
        return cleaned

def build_api_raw_account_lookup(raw_report_data):
    lookup = {}
    if not isinstance(raw_report_data, dict):
        return lookup

    xml_report = raw_report_data.get('xmlJsonResponse', {})
    accounts = xml_report.get('caisAccount', {}).get('caisAccountDetails', [])
    if not isinstance(accounts, list):
        return lookup

    for raw_account in accounts:
        if not isinstance(raw_account, dict):
            continue
        account_number = clean_str(raw_account.get('accountNumber'))
        if account_number and account_number not in lookup:
            lookup[account_number] = raw_account
    return lookup

def transform_api_account(account, raw_account):
    if not isinstance(account, dict):
        return None

    raw_account = raw_account if isinstance(raw_account, dict) else {}
    suit_filed_status = normalize_api_suit_filed_status(
        raw_account.get('suitFiledWillfulDefaultWrittenOffStatus') or raw_account.get('suitFiledWilfulDefault')
    )
    payment_history = normalize_api_payment_history(account.get('paymentHistory', []), suit_filed_status=suit_filed_status)

    credit_limit = clean_money(raw_account.get('creditLimitAmount'))
    sanctioned_amount = credit_limit if credit_limit > 0 else clean_money(account.get('sanctioned'))
    no_write_off_amount = clean_money(raw_account.get('originalChargeOffAmount'))
    if no_write_off_amount <= 0:
        no_write_off_amount = clean_money(raw_account.get('settlementAmount'))

    detailed_emi = clean_money(account.get('emi'))
    raw_emi = clean_money(raw_account.get('scheduledMonthlyPaymentAmount'))

    transformed = {
        'provider': clean_str(account.get('provider')) or clean_str(raw_account.get('subscriberName')),
        'accountType': clean_str(account.get('productName')),
        'sanctionedAmount': sanctioned_amount,
        'totalSanctionAmt': clean_money(raw_account.get('highestCreditOrOrignalLoanAmount')),
        'outstanding': clean_money(account.get('outstanding')),
        'totalBalance': clean_money(raw_account.get('currentBalance')),
        'paidPrincipal': clean_money(account.get('paidPrincipal')),
        'emi': detailed_emi if detailed_emi > 0 else (raw_emi if raw_emi > 0 else 0),
        'paymentHistory': payment_history,
        'repaymentTenure': normalize_positive_tenure(raw_account.get('repaymentTenure')),
        'accountOpenDate': parse_flexible_date(account.get('accountOpenDate') or raw_account.get('openDate')),
        'accountCloseDate': parse_flexible_date(account.get('accountCloseDate') or raw_account.get('dateClosed')),
        'accountStatus': normalize_api_account_status(account.get('accountStatus')),
        'lastPaymentDate': parse_flexible_date(raw_account.get('dateOfLastPayment')),
        'lastPaymentAmount': clean_money(raw_account.get('valueOfCreditsLastMonth')),
        'accountPastDueAmount': clean_money(raw_account.get('amountPastDue')),
        'writtenOffAmtTotal': clean_money(raw_account.get('writtenOffAmtTotal')),
        'noWriteOff': no_write_off_amount
    }

    return transformed

def build_qfinance_like_payload_from_api(report_data, raw_report_data, pan):
    if not isinstance(report_data, dict):
        return None

    detailed_report = report_data.get('detailedReport', {})
    if not isinstance(detailed_report, dict):
        return None

    raw_account_lookup = build_api_raw_account_lookup(raw_report_data)
    transformed_credit_cards = []
    transformed_loans = {}
    transformed_other_loans = []
    transformed_others = {}

    for account in detailed_report.get('cards', []) or []:
        transformed = transform_api_account(account, raw_account_lookup.get(clean_str(account.get('accountNumber'))))
        if transformed:
            transformed_credit_cards.append(transformed)

    for loan_type, accounts in (detailed_report.get('loans') or {}).items():
        if not isinstance(accounts, list):
            continue
        transformed_accounts = []
        for account in accounts:
            transformed = transform_api_account(account, raw_account_lookup.get(clean_str(account.get('accountNumber'))))
            if transformed:
                transformed_accounts.append(transformed)
        if loan_type == 'otherLoans':
            transformed_other_loans.extend(transformed_accounts)
        else:
            transformed_loans[loan_type] = transformed_accounts

    for section_name, accounts in (detailed_report.get('others') or {}).items():
        if not isinstance(accounts, list):
            continue
        transformed_accounts = []
        for account in accounts:
            transformed = transform_api_account(account, raw_account_lookup.get(clean_str(account.get('accountNumber'))))
            if transformed:
                transformed_accounts.append(transformed)
        transformed_others[section_name] = transformed_accounts

    enquiries = detailed_report.get('enquiries', {}) if isinstance(detailed_report.get('enquiries'), dict) else {}
    normalized_recent = normalize_api_enquiries(enquiries.get('recent', []))
    normalized_all = normalize_api_enquiries(enquiries.get('all', []))

    summary = {}
    if isinstance(enquiries.get('summary'), dict):
        summary.update(enquiries.get('summary'))
    total_caps_summary = raw_report_data.get('xmlJsonResponse', {}).get('totalCAPSSummary', {}) if isinstance(raw_report_data, dict) else {}
    if isinstance(total_caps_summary, dict):
        summary.setdefault('totalCAPSLast30Days', total_caps_summary.get('totalCAPSLast30Days'))
        summary.setdefault('totalCAPSLast90Days', total_caps_summary.get('totalCAPSLast90Days'))

    return {
        'data': {
            'reportData': {
                'reportSummary': {
                    'personalDetails': {'pan': pan},
                    'enquiries': {
                        'recent': normalized_recent,
                        'all': normalized_all,
                        'summary': summary
                    }
                },
                'creditAnalysis': {
                    'creditCards': transformed_credit_cards,
                    'loans': transformed_loans,
                    'otherLoans': transformed_other_loans,
                    'others': transformed_others,
                    'enquiries': {
                        'recent': normalized_recent,
                        'all': normalized_all,
                        'summary': summary
                    }
                }
            }
        }
    }

def fetch_api_server_view_fallback_rows(cursor, report_ids, requested_pans):
    if not report_ids:
        return [], []

    report_placeholders = _build_in_clause(report_ids)
    tradelines_query = f"""
        SELECT
            pan,
            Institution,
            account_type,
            Balance,
            past_due_amount,
            last_payment,
            last_payment_date,
            account_status,
            sanction_amount,
            credit_limit,
            installment_amount,
            repayment_tenure,
            date_opened,
            date_closed,
            written_off_amt_total,
            write_offs,
            report_id
        FROM api_server.vw1_customer_credit_lines
        WHERE report_id IN ({report_placeholders})
        ORDER BY pan, created_at DESC
    """
    cursor.execute(tradelines_query, report_ids)
    tradeline_rows = cursor.fetchall()

    rows_by_pan = {pan: [] for pan in requested_pans}
    for (
        pan, institution, account_type, balance, past_due_amount, last_payment,
        last_payment_date, account_status, sanction_amount, credit_limit,
        installment_amount, repayment_tenure, date_opened, date_closed,
        written_off_amt_total, write_offs, report_id
    ) in tradeline_rows:
        normalized_pan = str(pan).strip().upper()
        if normalized_pan not in rows_by_pan:
            continue

        sanctioned_amt = clean_money(credit_limit)
        if sanctioned_amt <= 0:
            sanctioned_amt = clean_money(sanction_amount)

        outstanding_amt = clean_money(balance)
        overdue_amount = clean_money(past_due_amount)
        status_raw = clean_str(account_status)
        written_off_amount = clean_money(written_off_amt_total)
        written_off_flag = "Yes" if written_off_amount > 0 or clean_nullable_str(write_offs) else "No"

        row = {
            'pan': normalized_pan,
            'fiName': clean_str(institution),
            'creditLineType': clean_str(account_type),
            'totalSanctionedAmount': sanctioned_amt,
            'currentOutstanding': outstanding_amt,
            'status': status_raw,
            'SuitFiled': "No",
            'SuitFiledStatus': None,
            'WrittenOffFlag': written_off_flag,
            'WrittenOffAmount': written_off_amount,
            'paidPrincipalAmount': max(0, sanctioned_amt - outstanding_amt),
            'EMI': clean_money(installment_amount),
            'totalTenure': clean_str(repayment_tenure),
            'pendingTenure': get_pending_tenure(repayment_tenure, parse_flexible_date(date_opened)),
            'startDate': parse_flexible_date(date_opened),
            'Balance': outstanding_amt,
            'lastPaymentDate': parse_flexible_date(last_payment_date),
            'lastPaymentAmount': clean_money(last_payment),
            'accountPastDueAmount': overdue_amount,
            'OverdueAmount': overdue_amount,
            'totalDelinquencies': 0,
            'delinquencies': '',
            'delinquencies30Days': 0,
            'delinquencies60Days': 0,
            'delinquencies90Days': 0,
            'Recent_Missed_30DPD': 0,
            'Recent_Missed_60DPD': 0,
            'Recent_Missed_90DPD': 0,
            'Enq_30Days': 0,
            'Enq_60Days': 0,
            'Enq_90Days': 0,
            'Enq_1Year': 0,
            'currentDpd': overdue_amount,
            'settledLast30Days': 0,
            'settledLast60Days': 0,
            'settledLast90Days': 0
        }

        rows_by_pan[normalized_pan].append(row)

    all_rows = []
    hits = []
    for pan in requested_pans:
        if rows_by_pan.get(pan):
            all_rows.extend(rows_by_pan[pan])
            hits.append(pan)

    return all_rows, sorted(set(hits))

def fetch_api_server_fallback_rows(cursor, specific_pans):
    if not specific_pans:
        return [], []

    normalized_pans = [str(p).strip().upper() for p in specific_pans if p and str(p).strip()]
    if not normalized_pans:
        return [], []

    placeholders = _build_in_clause(normalized_pans)
    latest_reports_query = f"""
        SELECT cr.panNumber, cr.id, cr.reportData, cr.rawReportData, cr.createdAt
        FROM api_server.credit_reports cr
        INNER JOIN (
            SELECT UPPER(TRIM(panNumber)) AS pan_key, MAX(createdAt) AS max_created
            FROM api_server.credit_reports
            WHERE status = 'SUCCESS'
              AND UPPER(TRIM(panNumber)) IN ({placeholders})
            GROUP BY UPPER(TRIM(panNumber))
        ) latest
            ON UPPER(TRIM(cr.panNumber)) = latest.pan_key
           AND cr.createdAt = latest.max_created
        WHERE cr.status = 'SUCCESS'
    """
    cursor.execute(latest_reports_query, normalized_pans)
    latest_reports = cursor.fetchall()

    if not latest_reports:
        return [], []

    rows_by_pan = {}
    report_ids = []
    for pan, report_id, report_data_raw, raw_report_data_raw, _ in latest_reports:
        normalized_pan = str(pan).strip().upper()
        report_ids.append(report_id)
        try:
            report_data = json.loads(report_data_raw) if isinstance(report_data_raw, str) else report_data_raw
            raw_report_data = json.loads(raw_report_data_raw) if isinstance(raw_report_data_raw, str) else raw_report_data_raw
            transformed_payload = build_qfinance_like_payload_from_api(report_data, raw_report_data, normalized_pan)
            rows_by_pan[normalized_pan] = process_single_record(transformed_payload, pan_from_db=normalized_pan)
        except Exception:
            rows_by_pan[normalized_pan] = []

    unresolved_pans = [pan for pan in normalized_pans if not rows_by_pan.get(pan)]
    if unresolved_pans:
        view_rows, view_hits = fetch_api_server_view_fallback_rows(cursor, report_ids, unresolved_pans)
        if view_hits:
            view_map = {pan: [] for pan in view_hits}
            for row in view_rows:
                view_map[row['pan']].append(row)
            for pan in view_hits:
                rows_by_pan[pan] = view_map.get(pan, [])

    all_rows = []
    fallback_hits = []
    for pan in normalized_pans:
        if rows_by_pan.get(pan):
            all_rows.extend(rows_by_pan[pan])
            fallback_hits.append(pan)

    return all_rows, sorted(set(fallback_hits))

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

        enquiry_summary = {}
        if isinstance(ce_section, dict) and isinstance(ce_section.get('summary'), dict):
            enquiry_summary.update(ce_section.get('summary'))
        if isinstance(re_section, dict) and isinstance(re_section.get('summary'), dict):
            enquiry_summary.update(re_section.get('summary'))

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
        enq_30 = get_enquiry_summary_count(enquiry_summary, 'last30Days', 'last30', 'totalCAPSLast30Days')
        enq_60 = get_enquiry_summary_count(enquiry_summary, 'last60Days', 'last60', 'totalCAPSLast60Days')
        enq_90 = get_enquiry_summary_count(enquiry_summary, 'last90Days', 'last90', 'totalCAPSLast90Days')
        enq_365 = get_enquiry_summary_count(enquiry_summary, 'last365Days', 'last1Year', 'totalCAPSLast365Days')

        if enq_30 is None: enq_30 = calculate_enquiries(enq_list, 30)
        if enq_60 is None: enq_60 = calculate_enquiries(enq_list, 60)
        if enq_90 is None: enq_90 = calculate_enquiries(enq_list, 90)
        if enq_365 is None: enq_365 = calculate_enquiries(enq_list, 365)

        # ACCOUNTS
        all_accounts = []

        credit_cards = credit_analysis.get('creditCards', [])
        if isinstance(credit_cards, list):
            all_accounts.extend(credit_cards)

        loans_data = credit_analysis.get('loans', {})
        if isinstance(loans_data, dict):
            for _, val in loans_data.items():
                if isinstance(val, list):
                    all_accounts.extend(val)
        elif isinstance(loans_data, list):
            all_accounts.extend(loans_data)

        # Newer report payloads place many consumer/retail tradelines here.
        other_loans = credit_analysis.get('otherLoans', [])
        if isinstance(other_loans, list):
            all_accounts.extend(other_loans)

        others = credit_analysis.get('others', {})
        if isinstance(others, dict):
            overdraft_accounts = others.get('overdraft', [])
            if isinstance(overdraft_accounts, list):
                all_accounts.extend(overdraft_accounts)
        
        if not all_accounts:
             row = {header: None for header in TARGET_HEADERS}
             row['pan'] = pan
             row['SuitFiled'] = "No"
             row['SuitFiledStatus'] = None
             row['WrittenOffFlag'] = "No"
             row['WrittenOffAmount'] = 0
             row['OverdueAmount'] = 0
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

            payment_history = account.get('paymentHistory', [])
            delinq_stats = get_delinquency_buckets(payment_history)
            suit_filed_flag, suit_filed_status = get_suit_filed_info(payment_history)
            
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
            written_off_flag, written_off_amount = get_written_off_info(account, status_raw=status_raw)
            overdue_amount = clean_money(account.get('accountPastDueAmount'))
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
                'SuitFiled': suit_filed_flag,
                'SuitFiledStatus': suit_filed_status,
                'WrittenOffFlag': written_off_flag,
                'WrittenOffAmount': written_off_amount,
                'paidPrincipalAmount': paid_principal,
                'EMI': clean_money(account.get('emi')),
                'totalTenure': clean_str(total_tenure_raw),
                'pendingTenure': pending_tenure,
                'startDate': clean_str(open_date_raw),
                'Balance': outstanding_amt,
                'lastPaymentDate': clean_str(account.get('lastPaymentDate')),
                'lastPaymentAmount': clean_money(account.get('lastPaymentAmount')),
                'accountPastDueAmount': overdue_amount,
                'OverdueAmount': overdue_amount,
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
                'currentDpd': overdue_amount,
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
        query_params = None
        # 1. BUILD QUERY
        if specific_pans and len(specific_pans) > 0:
            msg = f"Fetching records for {len(specific_pans)} specific PANs..."
            print(msg)
            if progress_callback: progress_callback(0, 0, msg)

            normalized_pans = [str(p).strip().upper() for p in specific_pans if p and str(p).strip()]
            placeholders = _build_in_clause(normalized_pans)
            query = f"SELECT pancardNumber, recommendationJsonFile FROM qfinance.q_report WHERE UPPER(TRIM(pancardNumber)) IN ({placeholders}) ORDER BY createdAt DESC"
            query_params = normalized_pans
            
        else:
            msg = "Fetching ALL records from database..."
            print(msg)
            if progress_callback: progress_callback(0, 0, msg)
            
            query = "SELECT pancardNumber, recommendationJsonFile FROM qfinance.q_report ORDER BY createdAt DESC"
        
        cursor.execute(query, query_params or ())
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

        fallback_rows = []
        fallback_pans = []
        if specific_pans and len(specific_pans) > 0:
            normalized_requested = [str(p).strip().upper() for p in specific_pans if p and str(p).strip()]
            missing_pans = [p for p in normalized_requested if p not in seen_pans]
            if missing_pans:
                fallback_msg = f"Falling back to api_server for {len(missing_pans)} PAN(s) missing in qfinance..."
                print(fallback_msg)
                if progress_callback: progress_callback(0, max(total_tasks, 1), fallback_msg)
                fallback_rows, fallback_pans = fetch_api_server_fallback_rows(cursor, missing_pans)
                if fallback_pans:
                    print(f"api_server fallback returned data for: {', '.join(fallback_pans)}")
                else:
                    print("api_server fallback returned no matching tradelines.")

        if total_tasks == 0 and not fallback_rows:
            if progress_callback: progress_callback(0, 0, "No records found matching criteria.")
            conn.close()
            return None

        if total_tasks > 0 and progress_callback:
            progress_callback(0, total_tasks, f"Starting Parallel Processing for {total_tasks} Tasks...")
        
        # 3. PARALLEL EXECUTION
        start_time = time.time()
        
        if total_tasks > 0:
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

        if fallback_rows:
            all_final_rows.extend(fallback_rows)
            if progress_callback:
                progress_callback(total_tasks, max(total_tasks, 1), f"api_server fallback added data for {len(fallback_pans)} PAN(s).")
                    
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
