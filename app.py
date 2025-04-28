import os
import logging
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from fpdf import FPDF
import smtplib
from email.message import EmailMessage
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors

# -----------------
# Setup Logging
# -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# -----------------
# Load environment variables
# -----------------
load_dotenv()

# -----------------
# Configuration
# -----------------
PG_CONFIG = {
    'host': os.getenv('PG_HOST'),
    'port': int(os.getenv('PG_PORT', 5432)),
    'dbname': os.getenv('PG_DATABASE'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD')
}

EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
EMAIL_RECIPIENTS = os.getenv('EMAIL_RECIPIENTS', '').split(',')

# -----------------
# SQL Queries
# -----------------
SQL1 = """ WITH period AS (
  SELECT
    (EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days') * 1000)::bigint AS ts_min,
    (EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours') * 1000)::bigint AS ts_now,
    (EXTRACT(EPOCH FROM NOW() - INTERVAL '8 days') * 1000)::bigint AS ts_wow
)
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
  cr.approve_count_7d,
  cr.approve_count_wow,
  cr.approve_count_now,
  cr.approve_count_global,
  cr.decline_count_7d,
  cr.decline_count_wow,
  cr.decline_count_now,
  cr.decline_count_global,
  COALESCE(cr.return_value_7d, 0) / NULLIF(pa.order_value_7d, 0) AS return_rate_7d,
  COALESCE(cr.return_value_wow, 0) / NULLIF(pa.order_value_wow, 0) AS return_rate_wow,
  COALESCE(cr.return_value_now, 0) / NULLIF(pa.order_value_now, 0) AS return_rate_now,
  COALESCE(cr.return_value_global, 0) / NULLIF(pa.order_value_global, 0) AS return_rate_global,
  pa.review_count_7d,
  pa.review_count_wow,
  pa.review_count_now,
  pa.review_count_global,
  pa.review_count_7d::numeric / NULLIF(pa.order_count_7d, 0) AS review_rate_7d,
  pa.review_count_wow::numeric / NULLIF(pa.order_count_wow, 0) AS review_rate_wow,
  pa.review_count_now::numeric / NULLIF(pa.order_count_now, 0) AS review_rate_now,
  pa.review_count_global::numeric / NULLIF(pa.order_count_global, 0) AS review_rate_global
FROM (
  SELECT
    merchant_id,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_min FROM period)) AS order_count_7d,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_wow FROM period) and timestamp <= (SELECT ts_min FROM period)) AS order_count_wow,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_now FROM period)) AS order_count_now,
    COUNT(*) AS order_count_global,
    SUM(amount) FILTER (WHERE timestamp >= (SELECT ts_min FROM period)) AS order_value_7d,
    SUM(amount) FILTER (WHERE timestamp >= (SELECT ts_wow FROM period) and timestamp <= (SELECT ts_min FROM period)) AS order_value_wow,
    SUM(amount) FILTER (WHERE timestamp >= (SELECT ts_now FROM period)) AS order_value_now,
    SUM(amount) AS order_value_global,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_min FROM period) AND decision = 'REVIEW') AS review_count_7d,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_wow FROM period) and timestamp <= (SELECT ts_min FROM period) AND decision = 'REVIEW') AS review_count_wow,
    COUNT(*) FILTER (WHERE timestamp >= (SELECT ts_now FROM period) AND decision = 'REVIEW') AS review_count_now,
    COUNT(*) FILTER (WHERE decision = 'REVIEW') AS review_count_global,
    COUNT(DISTINCT customer_id) FILTER (WHERE timestamp >= (SELECT ts_min FROM period)) AS unique_customers_7d,
    COUNT(DISTINCT customer_id) FILTER (WHERE timestamp >= (SELECT ts_wow FROM period) and timestamp <= (SELECT ts_min FROM period)) AS unique_customers_wow,
    COUNT(DISTINCT customer_id) FILTER (WHERE timestamp >= (SELECT ts_now FROM period)) AS unique_customers_now,
    COUNT(DISTINCT customer_id) AS unique_customers_global
  FROM purchase
  WHERE merchant_id = """
SQL2 = """ 
GROUP BY merchant_id
) pa
JOIN (
  SELECT
    co.merchant_id,
    COUNT(DISTINCT co.id) FILTER (WHERE p.timestamp >= (SELECT ts_min FROM period)) AS order_line_count_7d,
    COUNT(DISTINCT co.id) FILTER (WHERE p.timestamp >= (SELECT ts_wow FROM period) and p.timestamp <= (SELECT ts_min FROM period)) AS order_line_count_wow,
    COUNT(DISTINCT co.id) FILTER (WHERE p.timestamp >= (SELECT ts_now FROM period)) AS order_line_count_now,
    COUNT(DISTINCT co.id) AS order_line_count_global
  FROM purchase p
  JOIN cart_item co ON co.purchase_order = p.order_id
  WHERE p.merchant_id =  """
SQL3 = """ 
GROUP BY co.merchant_id
) ola ON pa.merchant_id = ola.merchant_id
LEFT JOIN (
  SELECT
    rr.merchant_id,
    SUM(rd.amount * rd.quantity) FILTER (WHERE rr.initiated_at >= (SELECT ts_min FROM period)) AS return_value_7d,
    SUM(rd.amount * rd.quantity) FILTER (WHERE rr.initiated_at >= (SELECT ts_wow FROM period) and rr.initiated_at <= (SELECT ts_min FROM period)) AS return_value_wow,
    SUM(rd.amount * rd.quantity) FILTER (WHERE rr.initiated_at >= (SELECT ts_now FROM period)) AS return_value_now,
    SUM(rd.amount * rd.quantity) AS return_value_global,
    COUNT(DISTINCT rr.return_id) FILTER (WHERE rr.initiated_at >= (SELECT ts_min FROM period)) AS return_count_7d,
    COUNT(DISTINCT rr.return_id) FILTER (WHERE rr.initiated_at >= (SELECT ts_wow FROM period) and rr.initiated_at <= (SELECT ts_min FROM period)) AS return_count_wow,
    COUNT(DISTINCT rr.return_id) FILTER (WHERE rr.initiated_at >= (SELECT ts_now FROM period)) AS return_count_now,
    COUNT(DISTINCT rr.return_id) AS return_count_global,
    count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_min FROM period) and rr.review_status = 1) AS approve_count_7d,
    count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_wow FROM period) and rr.updated_at <= (SELECT ts_min FROM period) and rr.review_status = 1) AS approve_count_wow,
    count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_now FROM period) and rr.review_status = 1) AS approve_count_now,
    count(*) FILTER (where rr.review_status = 1) As approve_count_global,
    count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_min FROM period) and rr.review_status = 2) AS decline_count_7d,
    count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_wow FROM period) and rr.updated_at <= (SELECT ts_min FROM period) and rr.review_status = 2) AS decline_count_wow,
    count(*) FILTER (WHERE rr.updated_at >= (SELECT ts_now FROM period) and rr.review_status = 2) AS decline_count_now,
    count(*) FILTER (where rr.review_status = 2) As decline_count_global
  FROM return_request rr
  JOIN return_details rd ON rd.return_id = rr.return_id
  WHERE rr.merchant_id =  """
SQL4 = """ 
GROUP BY rr.merchant_id
) cr ON pa.merchant_id = cr.merchant_id; """

# -----------------
# Functions
# -----------------

def fetch_and_save_local():
    logger.info("Starting data fetch and save locally.")
    db_url = f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}?hostaddr={PG_CONFIG['host']}"
    engine = create_engine(db_url)

    merchant_ids = [13927,13918,13916,13529,13632,13652,13509,13497,13934,13546,13552,13504,13488,13494,13483,13502,13387,13510,13503,12302]
    all_metrics = []

    with engine.connect() as conn:
        for mid in merchant_ids:
            merchant_sql = (
                SQL1 + str(mid) + SQL2 + str(mid) + SQL3 + str(mid) + SQL4
            )   
            df_mid = pd.read_sql(merchant_sql, engine)

            row = conn.execute(
                text("SELECT name FROM merchant WHERE id = :mid"),
                {"mid": mid}
            ).fetchone()
            merchant_name = row[0] if row else "Unknown"

            logger.info(f"Query executed and merchant fetched for ID: {mid} ({merchant_name})")
            if not df_mid.empty:
                orig = df_mid.iloc[0]
                s = orig.copy()
                s["merchant_name"] = merchant_name
                all_metrics.append(s)
            else:
                logger.warning(f"No data found for merchant_id {mid}")

    df = pd.DataFrame(all_metrics)

    if not df.empty:
        csv_path = f"daily_merchant_metrics_{pd.Timestamp.today().date()}.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"Data saved to {csv_path}")
    else:
        logger.warning("Fetched DataFrame is empty. Nothing saved.")

    return df


def generate_pdf(df, output_path="merchant_metrics.pdf"):
    logger.info("Starting PDF generation.")
    df = df.fillna("N/A")
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4

    c.setFont("Helvetica-Bold", 24)
    c.drawCentredString(width / 2, height / 2, "Live Monitoring Report")
    c.showPage()

    time_suffixes = ["_now", "_7d", "_wow", "_global"]
    header = ["Metric", "Last 24 hours", "Last 7 Days", "WoW - Last 24 hrs", "Lifetime"]

    def group_metrics(columns):
        base = {}
        for col in columns:
            for suf in time_suffixes:
                if col.endswith(suf):
                    name = col[:-len(suf)]
                    base.setdefault(name, {})[suf] = col
                    break
        return base

    grouped_metrics = group_metrics(df.columns)

    for _, row in df.iterrows():
        merchant_id = row["merchant_id"]
        merchant_name = row["merchant_name"]
        logger.info(f"Adding merchant {merchant_name} (ID: {merchant_id}) to PDF.")

        c.setFont("Helvetica-Bold", 16)
        c.drawString(1 * inch, height - 1 * inch, f"{merchant_name} (Merchant ID: {merchant_id})")
        y = height - 1.5 * inch
        x = 0.7 * inch
        table_w = width - 2 * x
        col_w = [table_w * 0.25] + [table_w * 0.1875] * 4

        hdr_tbl = Table([header], colWidths=col_w)
        hdr_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
        ]))
        hdr_tbl.wrapOn(c, width, height)
        hdr_h = hdr_tbl._rowHeights[0]
        hdr_tbl.drawOn(c, x, y - hdr_h)
        y -= hdr_h + 10

        for metric, cols in grouped_metrics.items():
            vals = [metric.replace("_", " ").title()]
            now_val = row.get(cols.get("_now")) if "_now" in cols else None

            for suf in time_suffixes:
                col = cols.get(suf)
                if suf == "_wow":
                    wow_val = row.get(col) if col else None
                    if isinstance(wow_val, float) and isinstance(now_val, float):
                        diff = wow_val - now_val
                        val = f"{diff:+,.2f}" if diff else (f"{int(diff):,}" if "count" in metric else f"{diff:,.2f}")
                    else:
                        val = "N/A"
                else:
                    v = row.get(col) if col else None
                    if isinstance(v, float):
                        val = f"{int(v):,}" if "count" in metric else f"{v:,.2f}"
                    elif v == "N/A" or pd.isna(v):
                        val = "N/A"
                    else:
                        val = f"{int(v):,}"

                if "rate" in metric and val.replace(",", "").replace(".", "").isdigit():
                    val = f"{val}%"
                elif "value" in metric and val.replace(",", "").replace(".", "").isdigit():
                    val = f"${val}"

                vals.append(val)

            val_tbl = Table([vals], colWidths=col_w)
            val_tbl.setStyle(TableStyle([
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
            ]))

            val_tbl.wrapOn(c, width, height)
            row_h = val_tbl._rowHeights[0]
            if y - row_h < inch:
                c.showPage()
                y = height - 1.5 * inch
                c.setFont("Helvetica-Bold", 16)
                c.drawString(x, height - 1 * inch, f"Merchant ID: {merchant_id}")
                y -= 30
                hdr_tbl.drawOn(c, x, y - hdr_h)
                y -= hdr_h + 10

            val_tbl.drawOn(c, x, y - row_h)
            y -= row_h + 5

        c.showPage()

    c.save()
    logger.info(f"PDF generated at {output_path}")
    return os.path.abspath(output_path)


def send_email_report(pdfs):
    logger.info("Starting to send email report.")
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
    logger.info('Email sent successfully.')


def scheduled_job():
    logger.info("Scheduled job started.")
    df = fetch_and_save_local()
    if not df.empty:
        pdfs = generate_pdf(df)
        send_email_report(pdfs)
        # send_slack_dm(pdfs)
    else:
        logger.warning("No data available for today's date. Skipping PDF/email generation.")

# -----------------
# Scheduler Setup
# -----------------
if __name__ == '__main__':
    scheduled_job()
    scheduler = BlockingScheduler()
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped manually.")
