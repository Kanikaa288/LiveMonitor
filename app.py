import os
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine
import pg8000
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from fpdf import FPDF
import smtplib
from email.message import EmailMessage
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Load environment variables
load_dotenv()

# -----------------
# Configuration
# -----------------
# PostgreSQL connection (not used directly for PDF/email)
PG_CONFIG = {
    'host': os.getenv('PG_HOST'),
    'port': int(os.getenv('PG_PORT', 5432)),
    'dbname': os.getenv('PG_DATABASE'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD')
}

# Retention in days for BigQuery partition expiration
# PARTITION_EXPIRATION_DAYS = int(os.getenv('PARTITION_EXPIRATION_DAYS', 14))

# Email settings
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
EMAIL_RECIPIENTS = os.getenv('EMAIL_RECIPIENTS', '').split(',')



# Slack settings
SLACK_TOKEN = os.getenv('SLACK_TOKEN')
slack_client = WebClient(token=SLACK_TOKEN)

# -----------------
# SQL Query (multi-CTE) to fetch metrics
# -----------------
SQL = """
WITH
  -- 7-day cutoff in ms
  period AS (
    SELECT EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days') * 1000 AS ts_min
  ),

  -- 1) Order counts
  order_count AS (
    SELECT
      COUNT(order_id)
        FILTER (WHERE timestamp >= (SELECT ts_min FROM period)) AS order_count_7d,
      COUNT(order_id) AS order_count_global
    FROM purchase
  ),

  -- 2) Order line counts
  order_line_count AS (
    SELECT
      COUNT(DISTINCT co.id)
        FILTER (WHERE p.timestamp >= (SELECT ts_min FROM period)) AS order_line_count_7d,
      COUNT(DISTINCT co.id) AS order_line_count_global
    FROM purchase p
    JOIN cart_order co ON co.cart_id = p.cart_id
  ),

  -- 3) Return counts
  return_count AS (
    SELECT
      COUNT(return_id)
        FILTER (WHERE initiated_at >= (SELECT ts_min FROM period)) AS return_count_7d,
      COUNT(return_id) AS return_count_global
    FROM return_request
  ),

  -- 4) Order values
  order_value AS (
    SELECT
      SUM(amount)
        FILTER (WHERE timestamp >= (SELECT ts_min FROM period)) AS order_value_7d,
      SUM(amount) AS order_value_global
    FROM purchase
  ),

  -- 5) Return values
  return_value AS (
    SELECT
      SUM(rd.amount * rd.quantity)
        FILTER (WHERE rr.initiated_at >= (SELECT ts_min FROM period)) AS return_value_7d,
      SUM(rd.amount * rd.quantity) AS return_value_global
    FROM return_request rr
    JOIN return_details rd ON rd.return_id = rr.return_id
  ),

  -- 6) Return rates
  return_rate AS (
    SELECT
      COALESCE(rv.return_value_7d, 0) / NULLIF(ov.order_value_7d,    0) AS return_rate_value_7d,
      COALESCE(rv.return_value_global, 0) / NULLIF(ov.order_value_global, 0) AS return_rate_value_global
    FROM order_value ov
    CROSS JOIN return_value rv
  ),

  -- 7) Review counts
  review_counts AS (
    SELECT
      SUM(CASE
            WHEN timestamp >= (SELECT ts_min FROM period)
                 AND decision = 'review'
            THEN 1 ELSE 0
          END) AS review_count_7d,
      SUM(CASE WHEN decision = 'review' THEN 1 ELSE 0 END) AS review_count_global
    FROM purchase
  ),

  -- 8) Review rates
  review_rate AS (
    SELECT
      -- 7-day review rate
      SUM(CASE
            WHEN timestamp >= (SELECT ts_min FROM period)
                 AND decision = 'review'
            THEN 1 ELSE 0
          END)::numeric
        / NULLIF(
            SUM(CASE
                  WHEN timestamp >= (SELECT ts_min FROM period) THEN 1 ELSE 0
                END),
            0
          ) AS review_rate_7d,
      -- global review rate
      SUM(CASE WHEN decision = 'review' THEN 1 ELSE 0 END)::numeric
        / NULLIF(COUNT(*), 0) AS review_rate_global
    FROM purchase
  ),

  -- 9) VIP (repeat-customer) rates
  vip_rate AS (
    SELECT
      (SUM(cnt_7d) FILTER (WHERE cnt_7d > 1))::numeric
        / NULLIF(SUM(cnt_7d), 0) AS vip_rate_7d,
      (SUM(cnt)   FILTER (WHERE cnt   > 1))::numeric
        / NULLIF(SUM(cnt),   0) AS vip_rate_global
    FROM (
      SELECT
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_min FROM period)) AS cnt_7d
      FROM purchase
      GROUP BY customer_id
    ) sub
  ),

  -- 10) Workflow distributions
  workflow_distribution AS (
    SELECT
      JSON_OBJECT_AGG(triggered_workflow, cnt_7d)    AS workflow_dist_7d,
      JSON_OBJECT_AGG(triggered_workflow, cnt_global) AS workflow_dist_global
    FROM (
      SELECT
        triggered_workflow,
        COUNT(*) FILTER (
          WHERE timestamp >= (SELECT ts_min FROM period)
            AND decision = 'review'
        ) AS cnt_7d,
        COUNT(*) FILTER (WHERE decision = 'review') AS cnt_global
      FROM purchase
      WHERE decision = 'review'
        AND triggered_workflow IS NOT NULL
      GROUP BY triggered_workflow
    ) sub
  )

SELECT
  oc.order_count_7d,
  oc.order_count_global,
  ol.order_line_count_7d,
  ol.order_line_count_global,
  rc.return_count_7d,
  rc.return_count_global,
  ov.order_value_7d,
  ov.order_value_global,
  rv.return_value_7d,
  rv.return_value_global,
  rr.return_rate_value_7d,
  rr.return_rate_value_global,
  rvc.review_count_7d,
  rvc.review_count_global,
  rvr.review_rate_7d,
  rvr.review_rate_global,
  vip.vip_rate_7d,
  vip.vip_rate_global,
  wd.workflow_dist_7d,
  wd.workflow_dist_global
FROM order_count           oc
CROSS JOIN order_line_count ol
CROSS JOIN return_count     rc
CROSS JOIN order_value      ov
CROSS JOIN return_value     rv
CROSS JOIN return_rate      rr
CROSS JOIN review_counts    rvc
CROSS JOIN review_rate      rvr
CROSS JOIN vip_rate         vip
CROSS JOIN workflow_distribution wd;
"""

# -----------------
# Functions
# -----------------

def fetch_and_save_local():
    # Build a **bare** URL with no host/port in it
    db_url = f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}?hostaddr={PG_CONFIG['host']}"
    engine = create_engine(db_url)

    df = pd.read_sql(SQL, engine)
    csv_path = f"daily_merchant_metrics_{pd.Timestamp.today().date()}.csv"
    df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}")
    return df


# PDF generation class
class PDFReport(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 16)
        title = f"Live Monitoring Report - Merchant {self.merchant_id}"
        self.cell(0, 10, title, ln=True, align='C')
        self.ln(5)

    @property
    def epw(self):
        return self.w - 2 * self.l_margin

    def add_metrics_table(self, df):
        # Table headers
        headers = ['Metric', '7-Day Avg', 'Global Avg']
        self.set_font('Arial', 'B', 12)
        col_width = self.epw / len(headers)
        for h in headers:
            self.cell(col_width, 10, h, border=1)
        self.ln()

        # Table rows
        self.set_font('Arial', '', 10)
        metrics = [
            ('Order Count', 'order_count_7d', 'order_count_global'),
            ('Order Line Count', 'order_line_count_7d', 'order_line_count_global'),
            ('Return Count', 'return_count_7d', 'return_count_global'),
            ('Order Value', 'order_value_7d', 'order_value_global'),
            ('Return Value', 'return_value_7d', 'return_value_global'),
            ('Return Rate', 'return_rate_value_7d', 'return_rate_value_global'),
            ('Review Count', 'review_count_7d', 'review_count_global'),
            ('Review Rate', 'review_rate_7d', 'review_rate_global'),
            ('VIP Rate', 'vip_rate_7d', 'vip_rate_global'),
            ('Workflow Dist.', 'workflow_dist_7d', 'workflow_dist_global'),
        ]
        row = df.iloc[0]
        for label, c7, cg in metrics:
            self.cell(col_width, 8, label, border=1)
            self.cell(col_width, 8, str(row[c7]), border=1)
            self.cell(col_width, 8, str(row[cg]), border=1)
            self.ln()
        self.ln(5)

def generate_pdf_reports(df):
    paths = []
    for mid, grp in df.groupby('merchant_id'):
        pdf = PDFReport()
        pdf.merchant_id = mid
        pdf.add_page()
        pdf.add_metrics_table(grp.reset_index(drop=True))
        path = f"live_monitor_report_{mid}.pdf"
        pdf.output(path)
        paths.append(path)
    return paths

def send_email_report(pdf_paths):
    msg = EmailMessage()
    msg['Subject'] = 'Daily Live Monitoring Reports'
    msg['From'] = EMAIL_USER
    msg['To'] = ', '.join(EMAIL_RECIPIENTS)
    msg.set_content('Attached are the daily live monitoring reports.')

    for path in pdf_paths:
        with open(path, 'rb') as f:
            data = f.read()
            msg.add_attachment(data, maintype='application', subtype='pdf', filename=os.path.basename(path))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_USER, EMAIL_PASSWORD)
        smtp.send_message(msg)
    print('Email sent.')

def send_slack_dm(text):
    for email in EMAIL_RECIPIENTS:
        try:
            # 1. Lookup user ID by email
            resp = slack_client.users_lookupByEmail(email=email)
            user_id = resp['user']['id']

            # 2. Open or get DM channel
            dm = slack_client.conversations_open(users=user_id)
            dm_channel = dm['channel']['id']

            # 3. Send the message
            slack_client.chat_postMessage(channel=dm_channel, text=text)
            print(f"Sent DM to {email}")
        except SlackApiError as e:
            print(f"Slack error for {email}: {e.response['error']}")


def scheduled_job():
    df = fetch_and_save_local()
    if not df.empty:
        pdfs = generate_pdf_reports(df)
        send_email_report(pdfs)
        # send_slack_dm(pdfs)
    else:
        print("No data available for today's date. Skipping PDF/email generation.")

# -----------------
# Scheduler Setup
# -----------------
if __name__ == '__main__':
    # Run immediately once
    scheduled_job()
    # Then schedule future runs
    scheduler = BlockingScheduler()
    scheduler.add_job(scheduled_job, 'cron', hour=0, minute=0)  # daily at midnight UTC
    print('Scheduler started; ran immediately and scheduled daily runs.')
    scheduler.start()

