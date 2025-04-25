import os
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from fpdf import FPDF
import smtplib
from email.message import EmailMessage
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
import os
# from slack_sdk import WebClient
# from slack_sdk.errors import SlackApiError

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

# Email settings
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
EMAIL_RECIPIENTS = os.getenv('EMAIL_RECIPIENTS', '').split(',')


# Slack settings
# SLACK_TOKEN = os.getenv('SLACK_TOKEN')
# slack_client = WebClient(token=SLACK_TOKEN)

# -----------------
# SQL Query (multi-CTE) to fetch metrics
# -----------------
SQL = """
WITH period AS (
  SELECT EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days') * 1000 AS ts_min,
   EXTRACT(EPOCH FROM NOW() - INTERVAL '12 hours') * 1000 AS ts_now,
  EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days' - INTERVAL '12 hours') * 1000 AS ts_wow
),
purchase_agg AS (
  SELECT
    merchant_id,
    COUNT(*) FILTER (WHERE timestamp >= p.ts_min) AS order_count_7d,
	COUNT(*) FILTER (WHERE timestamp >= p.ts_wow) AS order_count_wow,
	COUNT(*) FILTER (WHERE timestamp >= p.ts_now) AS order_count_now,
    COUNT(*) AS order_count_global,
    SUM(amount) FILTER (WHERE timestamp >= p.ts_min) AS order_value_7d,
    SUM(amount) FILTER (WHERE timestamp >= p.ts_wow) AS order_value_wow,
    SUM(amount) FILTER (WHERE timestamp >= p.ts_now) AS order_value_now,
    SUM(amount) AS order_value_global,
    SUM(CASE WHEN timestamp >= p.ts_min AND decision = 'REVIEW' THEN 1 ELSE 0 END) AS review_count_7d,
    SUM(CASE WHEN timestamp >= p.ts_wow AND decision = 'REVIEW' THEN 1 ELSE 0 END) AS review_count_wow,
    SUM(CASE WHEN timestamp >= p.ts_now AND decision = 'REVIEW' THEN 1 ELSE 0 END) AS review_count_now,
    SUM(CASE WHEN decision = 'REVIEW' THEN 1 ELSE 0 END) AS review_count_global,
    COUNT(DISTINCT customer_id) FILTER (WHERE timestamp >= p.ts_min) AS unique_customers_7d,
    COUNT(DISTINCT customer_id) FILTER (WHERE timestamp >= p.ts_wow) AS unique_customers_wow,
    COUNT(DISTINCT customer_id) FILTER (WHERE timestamp >= p.ts_now) AS unique_customers_now,
    COUNT(DISTINCT customer_id) AS unique_customers_global
  FROM purchase, period p
  GROUP BY merchant_id
),
combined_returns AS (
  SELECT
  	rr.merchant_id,
    SUM(rd.amount * rd.quantity) FILTER (WHERE rr.initiated_at >= p.ts_min) AS return_value_7d,
    SUM(rd.amount * rd.quantity) FILTER (WHERE rr.initiated_at >= p.ts_wow) AS return_value_wow,
    SUM(rd.amount * rd.quantity) FILTER (WHERE rr.initiated_at >= p.ts_now) AS return_value_now,
    SUM(rd.amount * rd.quantity) AS return_value_global,
    COUNT(distinct rr.return_id) FILTER (WHERE rr.initiated_at >= p.ts_min) AS return_count_7d,
    COUNT(distinct rr.return_id) FILTER (WHERE rr.initiated_at >= p.ts_wow) AS return_count_wow,
    COUNT(distinct rr.return_id) FILTER (WHERE rr.initiated_at >= p.ts_now) AS return_count_now,
    COUNT(distinct rr.return_id) AS return_count_global
  FROM return_request rr
  JOIN return_details rd ON rd.return_id = rr.return_id
  CROSS JOIN period p
  group by rr.merchant_id
),
order_line_agg AS (
  SELECT
  co.merchant_id,
    COUNT(DISTINCT co.id) FILTER (WHERE p.timestamp >= period.ts_min) AS order_line_count_7d,
    COUNT(DISTINCT co.id) FILTER (WHERE p.timestamp >= period.ts_wow) AS order_line_count_wow,
    COUNT(DISTINCT co.id) FILTER (WHERE p.timestamp >= period.ts_now) AS order_line_count_now,
    COUNT(DISTINCT co.id) AS order_line_count_global
  FROM purchase p
  JOIN cart_item co ON co.purchase_order = p.order_id
  CROSS JOIN period
  GROUP BY co.merchant_id
)
-- ,

-- vip_data AS (
--   SELECT
--     (COUNT(*) FILTER (WHERE cnt_7d > 1 ))::numeric / NULLIF(COUNT(*), 0) AS vip_rate_7d,
--     (COUNT(*) FILTER (WHERE cnt_global > 1))::numeric / NULLIF(COUNT(*), 0) AS vip_rate_global
--   FROM (
--     SELECT
--       COUNT(*) FILTER (WHERE timestamp >= p.ts_min) AS cnt_7d,
--       COUNT(*) AS cnt_global
--     FROM purchase, period p
--     GROUP BY customer_id
--   ) customer_counts
-- ),
-- workflow_data AS (
--   SELECT
--     JSON_OBJECT_AGG(triggered_workflow, cnt_7d) AS workflow_dist_7d,
--     JSON_OBJECT_AGG(triggered_workflow, cnt_global) AS workflow_dist_global
--   FROM (
--     SELECT
--       triggered_workflow,
--       COUNT(*) FILTER (WHERE timestamp >= p.ts_min) AS cnt_7d,
--       COUNT(*) AS cnt_global
--     FROM purchase, period p
--     WHERE decision = 'REVIEW'
--       AND triggered_workflow IS NOT NULL
--     GROUP BY triggered_workflow
--   ) sub
-- )
SELECT
	pa.merchant_id,
  pa.order_count_7d,
  pa.order_count_wow,
  pa.order_count_now,
  pa.order_count_global,
  ola.order_line_count_7d,
  ola.order_line_count_wow,
  ola.order_line_count_now,
  ola.order_line_count_global,
  cr.return_count_7d,
  cr.return_count_wow,
  cr.return_count_now,
  cr.return_count_global,
  pa.order_value_7d,
  pa.order_value_wow,
  pa.order_value_now,
  pa.order_value_global,
  cr.return_value_7d,
  cr.return_value_wow,
  cr.return_value_now,
  cr.return_value_global,
  COALESCE(cr.return_value_7d, 0) / NULLIF(pa.order_value_7d, 0) AS return_rate_value_7d,
  COALESCE(cr.return_value_wow, 0) / NULLIF(pa.order_value_wow, 0) AS return_rate_value_wow,
  COALESCE(cr.return_value_now, 0) / NULLIF(pa.order_value_now, 0) AS return_rate_value_now,
  COALESCE(cr.return_value_global, 0) / NULLIF(pa.order_value_global, 0) AS return_rate_value_global,
  pa.review_count_7d,
  pa.review_count_wow,
  pa.review_count_now,
  pa.review_count_global,
  pa.review_count_7d::numeric / NULLIF(pa.order_count_7d, 0) AS review_rate_7d,
  pa.review_count_wow::numeric / NULLIF(pa.order_count_wow, 0) AS review_rate_wow,
  pa.review_count_now::numeric / NULLIF(pa.order_count_now, 0) AS review_rate_now,
  pa.review_count_global::numeric / NULLIF(pa.order_count_global, 0) AS review_rate_global
  -- ,
  -- vd.vip_rate_7d,
  -- vd.vip_rate_global,
  -- wd.workflow_dist_7d,
  -- wd.workflow_dist_global
FROM purchase_agg pa
CROSS JOIN order_line_agg ola
CROSS JOIN combined_returns cr
-- CROSS JOIN vip_data vd
-- CROSS JOIN workflow_data wd;
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
def generate_pdf(df, output_path="merchant_metrics.pdf"):
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4

    # --- Title Page ---
    c.setFont("Helvetica-Bold", 24)
    c.drawCentredString(width / 2, height / 2, "Live Monitoring Report")
    c.showPage()

    # --- Pages per Merchant ---
    for _, row in df.iterrows():
        merchant_id = row["merchant_id"]
        c.setFont("Helvetica-Bold", 16)
        c.drawString(1 * inch, height - 1 * inch, f"Merchant ID: {merchant_id}")

        c.setFont("Helvetica", 10)
        x_pos = 1 * inch
        y_pos = height - 1.5 * inch
        line_height = 14

        for col in df.columns:
            if col == "merchant_id":
                continue
            value = row[col]
            line = f"{col}: {value:.2f}" if isinstance(value, float) else f"{col}: {value}"
            c.drawString(x_pos, y_pos, line)
            y_pos -= line_height

            # Shift to next column if vertical space is exceeded
            if y_pos < 1 * inch:
                x_pos += 3.5 * inch
                y_pos = height - 1.5 * inch

        c.showPage()

    c.save()
    return os.path.abspath(output_path)


def send_email_report(pdfs):
    msg = EmailMessage()
    msg['Subject'] = 'Daily Live Monitoring Reports'
    msg['From'] = EMAIL_USER
    msg['To'] = ', '.join(EMAIL_RECIPIENTS)
    msg.set_content('Attached are the daily live monitoring reports.')

    
    with open(pdfs, 'rb') as f:
        data = f.read()
        msg.add_attachment(data, maintype='application', subtype='pdf', filename=os.path.basename(pdfs))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_USER, EMAIL_PASSWORD)
        smtp.send_message(msg)
    print('Email sent.')

# def send_slack_dm(text):
#     for email in EMAIL_RECIPIENTS:
#         try:
#             # 1. Lookup user ID by email
#             resp = slack_client.users_lookupByEmail(email=email)
#             user_id = resp['user']['id']

#             # 2. Open or get DM channel
#             dm = slack_client.conversations_open(users=user_id)
#             dm_channel = dm['channel']['id']

#             # 3. Send the message
#             slack_client.chat_postMessage(channel=dm_channel, text=text)
#             print(f"Sent DM to {email}")
#         except SlackApiError as e:
#             print(f"Slack error for {email}: {e.response['error']}")


def scheduled_job():
    df = fetch_and_save_local()
    if not df.empty:
        pdfs = generate_pdf(df)
        send_email_report(pdfs)
        # send_slack_dm(pdfs)
    else:
        print("No data available for today's date. Skipping PDF/email generation.")

# -----------------
# Scheduler Setup
# -----------------
if __name__ == '__main__':
    print("started scheduling")
    scheduled_job()
    scheduler = BlockingScheduler()
    scheduler.add_job(scheduled_job, 'interval', minutes=1)  # Run every 2 minutes
    print("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass

