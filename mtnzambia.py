\#!/usr/bin/env python3
import psycopg2
import smtplib
from datetime import date, timedelta
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# DB connection config
DB_CONFIG = {
    "host": "",
    "database": "db_lending_service",
    "user": "",
    "password": "",
    "port": 37822
}

# Gmail SMTP config
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "amos.tamabut@ezra.world"
SMTP_PASS = "fojkqknugmzphbgr"  # App password
#TO_EMAIL = "amos.tamabut@ezra.world"

# Multiple recipients
TO_EMAILS = [
    "amos.tamabut@ezra.world",
    "support@ezra.world",
    "dinko.svetic@ezra.world",
    "kevin.kariithi@ezra.world"
]

# Queries
QUERIES = {
    "all_due_yesterday": """
        SELECT *
        FROM tbl_loans
        WHERE due_date::date = CURRENT_DATE - INTERVAL '1 day';
    """,
    "due_yesterday_closed": """
        SELECT *
        FROM tbl_loans
        WHERE status = 'CLOSED'
          AND due_date::date = CURRENT_DATE - INTERVAL '1 day';
    """,
    "due_yesterday_open": """
        SELECT *
        FROM tbl_loans
        WHERE status = 'OPEN'
          AND due_date::date = CURRENT_DATE - INTERVAL '1 day';
    """,
    "due_yesterday_over_due": """
        SELECT *
        FROM tbl_loans
        WHERE status = 'OVER_DUE'
          AND due_date::date = CURRENT_DATE - INTERVAL '1 day';
    """,
    "actual_accruals_today": """
        WITH expected_loans AS (
            SELECT loan_id
            FROM tbl_loans
            WHERE status = 'OVER_DUE'
              AND due_date < CURRENT_DATE
              AND due_date >= CURRENT_DATE - INTERVAL '8 days'
        ),
        accruals_today AS (
            SELECT loan_id
            FROM loan_transactions
            WHERE transaction_type = 'ACCRUAL'
              AND transaction_status = 'SUCCESS'
              AND DATE(transaction_date) = CURRENT_DATE
        )
        SELECT DISTINCT e.loan_id
        FROM expected_loans e
        INNER JOIN accruals_today a
                ON e.loan_id = a.loan_id;
    """,
    "accruals_to_apply_detailed": """
        WITH candidate_loans AS (
            SELECT 
                l.loan_id,
                l.customer_id,
                l.due_date,
                CURRENT_DATE - l.due_date::date AS days_past_due
            FROM tbl_loans l
            WHERE l.status = 'OVER_DUE'
              AND l.created_by <> 'MIGRATION_SCRIPT'
              AND CURRENT_DATE > l.due_date::date
              AND CURRENT_DATE - l.due_date::date BETWEEN 1 AND 7
        )
        SELECT 
            cl.loan_id,
            cl.customer_id,
            cl.due_date,
            cl.days_past_due,
            CASE 
                WHEN lt.id IS NOT NULL THEN 'APPLIED'
                ELSE 'NOT_APPLIED'
            END AS accrual_status,
            lt.id AS accrual_txn_id,
            lt.amount AS accrual_amount,
            lt.transaction_date::date AS accrual_date
        FROM candidate_loans cl
        LEFT JOIN loan_transactions lt
               ON lt.loan_id = cl.loan_id
              AND lt.transaction_type = 'ACCRUAL'
              AND lt.transaction_status = 'SUCCESS'
              AND lt.transaction_date::date = CURRENT_DATE
        ORDER BY cl.days_past_due, cl.loan_id;
    """,
    "missed_accruals_yesterday": """
        SELECT 
            COUNT(*) FILTER (WHERE accrual > 0) AS accruals_applied,
            COUNT(*) FILTER (WHERE accrual = 0 OR accrual IS NULL) AS accruals_missing,
            COUNT(*) AS total_loans
        FROM (
            SELECT l.loan_id,
                   CASE 
                     WHEN t.loan_id IS NOT NULL THEN 1
                     ELSE 0
                   END AS accrual
            FROM tbl_loans l
            LEFT JOIN loan_transactions t
              ON l.loan_id = t.loan_id
             AND t.transaction_type = 'ACCRUAL'
             AND t.transaction_status = 'SUCCESS'
             AND DATE(t.transaction_date) = CURRENT_DATE
            WHERE l.due_date::date = CURRENT_DATE - INTERVAL '1 day'
              AND l.status = 'OVER_DUE'
        ) sub;
    """,
    "missed_accruals_2days": """
        SELECT 
            COUNT(*) FILTER (WHERE accrual > 0) AS accruals_applied,
            COUNT(*) FILTER (WHERE accrual = 0 OR accrual IS NULL) AS accruals_missing,
            COUNT(*) AS total_loans
        FROM tbl_loans
        WHERE due_date::date = (CURRENT_DATE - INTERVAL '2 day')
          AND status = 'OVER_DUE';
    """,
    # Sweep Queries
    "sweep_attempts_today": """
        SELECT l.loan_id
        FROM tbl_loans l
        JOIN loan_transactions_temp t ON l.loan_id = t.loan_id
        WHERE l.due_date < CURRENT_DATE
          AND (CURRENT_DATE - l.due_date::date) BETWEEN 1 AND 90
          AND t.transaction_type = 'SWEEP'
          AND DATE(t.created_on) = CURRENT_DATE;
    """,
    "sweep_success_yesterday": """
        SELECT loan_id
        FROM loan_transactions_temp
        WHERE transaction_type = 'SWEEP'
          AND transaction_status = 'SUCCESS'
          AND DATE(created_on) = (CURRENT_DATE - INTERVAL '1 day');
    """,
    # Loans overdue yesterday
    "overdue_yesterday_loans": """
        SELECT *
        FROM tbl_loans
        WHERE status = 'OVER_DUE'
          AND due_date::date = CURRENT_DATE - INTERVAL '1 day';
    """,
    # Loans overdue yesterday with sweep attempt yesterday
    "overdue_yesterday_with_sweep": """
        SELECT 
            l.loan_id,
            l.customer_id,
            l.due_date,
            l.principal,
            l.status AS loan_status,
            t.id AS transaction_id,
            t.created_on AS sweep_attempt_time,
            t.amount AS sweep_amount,
            t.transaction_status AS sweep_status
        FROM tbl_loans l
        LEFT JOIN loan_transactions_temp t
            ON l.loan_id = t.loan_id
           AND t.transaction_type = 'SWEEP'
           AND DATE(t.created_on) = (CURRENT_DATE - INTERVAL '1 day')
        WHERE l.due_date = (CURRENT_DATE - INTERVAL '1 day')
        ORDER BY l.loan_id, t.created_on DESC;
    """
}


def run_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()

def send_email(subject, body_html):
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = SMTP_USER
        #msg["To"] = TO_EMAIL
        msg["To"] = ", ".join(TO_EMAILS)   # must be string for header
        msg["Subject"] = subject

        msg.attach(MIMEText(body_html, "html"))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, TO_EMAILS, msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        report_date = date.today() - timedelta(days=1)

        # Run all queries
        results = {name: run_query(cursor, q) for name, q in QUERIES.items()}

        # Stats calculations
        total_all = len(results["all_due_yesterday"])
        total_closed = len(results["due_yesterday_closed"])
        total_open = len(results["due_yesterday_open"])
        total_over_due = len(results["due_yesterday_over_due"])

        expected_over_due = total_all - total_closed
        actual_over_due = total_over_due
        total_overdue_attempts = total_open + total_over_due

        success_pct = (total_over_due / total_overdue_attempts * 100) if total_overdue_attempts else 0
        failed_pct = (total_open / total_overdue_attempts * 100) if total_overdue_attempts else 0

        # Detailed accruals (1-7 DPD)
        accruals_detailed = results["accruals_to_apply_detailed"]
        total_accruals_to_apply = len(accruals_detailed)
        applied_count = sum(1 for row in accruals_detailed if row[4] == 'APPLIED')
        not_applied_count = total_accruals_to_apply - applied_count
        applied_pct_detailed = (applied_count / total_accruals_to_apply * 100) if total_accruals_to_apply else 0
        not_applied_pct_detailed = (not_applied_count / total_accruals_to_apply * 100) if total_accruals_to_apply else 0

        # Missed accruals yesterday
        accruals_applied_yest, accruals_missing_yest, total_loans_yest = results["missed_accruals_yesterday"][0]
        applied_pct = (accruals_applied_yest / total_loans_yest * 100) if total_loans_yest else 0
        missing_pct = (accruals_missing_yest / total_loans_yest * 100) if total_loans_yest else 0

        # Missed accruals 2 days ago
        accruals_applied_2d, accruals_missing_2d, total_loans_2d = results["missed_accruals_2days"][0]
        applied_pct_2d = (accruals_applied_2d / total_loans_2d * 100) if total_loans_2d else 0
        missing_pct_2d = (accruals_missing_2d / total_loans_2d * 100) if total_loans_2d else 0

        # Sweep stats
        total_sweep_attempts = len(results["sweep_attempts_today"])
        total_sweep_success = len(results["sweep_success_yesterday"])
        total_sweep_failed = total_sweep_attempts - total_sweep_success
        sweep_success_pct = (total_sweep_success / total_sweep_attempts * 100) if total_sweep_attempts else 0
        sweep_failed_pct = (total_sweep_failed / total_sweep_attempts * 100) if total_sweep_attempts else 0

        # Overdue yesterday sweep stats
        overdue_yest_loans = results["overdue_yesterday_loans"]
        overdue_yest_with_sweep = results["overdue_yesterday_with_sweep"]

        total_overdue_yest = len(overdue_yest_loans)
        total_overdue_yest_swept = len([r for r in overdue_yest_with_sweep if r[5] is not None])
        total_overdue_yest_unswept = total_overdue_yest - total_overdue_yest_swept

        swept_pct_yest = (total_overdue_yest_swept / total_overdue_yest * 100) if total_overdue_yest else 0
        unswept_pct_yest = (total_overdue_yest_unswept / total_overdue_yest * 100) if total_overdue_yest else 0

        # Table rows
        table_data = [
            ["Total loans due yesterday", total_all, "", ""],
            ["CLOSED", total_closed, "", ""],
            ["Expected OVER_DUE", expected_over_due, "", ""],
            ["Actual OVER_DUE", actual_over_due, "", ""],
            ["Loans still OPEN (should have OVER_DUE)", total_open, "", ""],
            ["Total overdue attempts", total_overdue_attempts, "", ""],
            ["Successful (OVER_DUE)", total_over_due, f"{success_pct:.2f}%", ""],
            ["Failed (still OPEN)", total_open, f"{failed_pct:.2f}%", ""],
            ["--- ACCRUALS DETAILED (1-7 DPD) ---", "", "", ""],
            ["Total loans needing accruals today", total_accruals_to_apply, "", ""],
            ["Accruals applied", applied_count, f"{applied_pct_detailed:.2f}%", ""],
            ["Accruals not applied", not_applied_count, f"{not_applied_pct_detailed:.2f}%", ""],
            ["--- ACCRUALS (those went over_due yesterday) ---", "", "", ""],
            ["Yesterday's loans with accruals applied", accruals_applied_yest, f"{applied_pct:.2f}%", ""],
            ["Yesterday's loans that missed accruals", accruals_missing_yest, f"{missing_pct:.2f}%", ""],
            ["--- ACCRUALS (Over due 2 DAYS AGO) ---", "", "", ""],
            ["Loans due 2 days ago", total_loans_2d, "", ""],
            ["Accruals applied (2 days ago)", accruals_applied_2d, f"{applied_pct_2d:.2f}%", ""],
            ["Accruals missed (2 days ago)", accruals_missing_2d, f"{missing_pct_2d:.2f}%", ""],
            ["--- SWEEPS (1-90 DPD, attempted yesterday) ---", "", "", ""],
            ["Total sweep attempts", total_sweep_attempts, "", ""],
            ["Successful sweeps", total_sweep_success, f"{sweep_success_pct:.2f}%", ""],
            ["Failed sweeps", total_sweep_failed, f"{sweep_failed_pct:.2f}%", ""],
            ["--- SWEEPS (Loans overdue yesterday) ---", "", "", ""],
            ["Total loans overdue yesterday", total_overdue_yest, "", ""],
            ["Overdue loans swept yesterday", total_overdue_yest_swept, f"{swept_pct_yest:.2f}%", ""],
            ["Overdue loans NOT swept yesterday", total_overdue_yest_unswept, f"{unswept_pct_yest:.2f}%", ""]
        ]

        # Convert to HTML manually so we can style section headers
        html_rows = ""
        for row in table_data:
            metric, count, pct, _ = row
            if metric.startswith("---"):  # Section header
                html_rows += f"""
                <tr style="background-color:#4CAF50; color:white; font-weight:bold;">
                    <td colspan="3">{metric.replace('---', '').strip()}</td>
                </tr>
                """
            else:
                html_rows += f"""
                <tr>
                    <td>{metric}</td>
                    <td>{count}</td>
                    <td>{pct}</td>
                </tr>
                """

        body_html = f"""
        <html>
        <head>
          <style>
            table {{
              border-collapse: collapse;
              width: 40%;
              font-family: Arial, sans-serif;
              font-size: 14px;
            }}
            th, td {{
              border: 1px solid #ddd;
              padding: 8px;
              text-align: left;
            }}
            th {{
              background-color: #4CAF50;
              color: white;
            }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
          </style>
        </head>
        <body>
          <h2>Loan  Report - {report_date}</h2>
          <table>
            <tr>
              <th>Metric</th>
              <th>Count</th>
              <th>Percentage</th>
            </tr>
            {html_rows}
          </table>
        </body>
        </html>
        """

        # Print debug table in console
        print(tabulate(table_data, headers=["Metric", "Count", "Percentage", ""], tablefmt="grid"))

        # Send HTML email
        subject = f"MTN Zambia Daily Jobs  Report - {report_date}"
        send_email(subject, body_html)

        cursor.close()
        conn.close()
        print("\nDone.\n")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
