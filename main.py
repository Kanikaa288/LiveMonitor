import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF
import smtplib
from email.message import EmailMessage
from apscheduler.schedulers.blocking import BlockingScheduler
import os
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Load environment variables from .env file
load_dotenv()

# Slack settings
SLACK_TOKEN = os.getenv('SLACK_TOKEN')
slack_client = WebClient(token=SLACK_TOKEN)

# ------------------------------------------------
# NOTE: Dummy metrics provided for demonstration
# ------------------------------------------------
# The following sections for database connection,
# data fetching, and metric calculations are commented out.

# # -----------------
# # Database Connection
# # -----------------
# def get_db_connection():
#     pass

# # -----------------
# # Data Fetching
# # -----------------
# def fetch_raw_data():
#     pass

# # -----------------
# # Metric Calculation
# # -----------------
# def calculate_metrics(df):
#     pass

# # -----------------
# # Store Metrics in GCP (BigQuery)
# # -----------------
# def store_in_bigquery(df):
#     pass

# ------------------------------------------------
# Dummy Data Initialization for Live Monitoring Metrics
# ------------------------------------------------
dummy_data = {
    'merchant_id': [101, 202],
    'order_count_7d_avg': [120, 150],
    'order_count_global_avg': [110, 140],
    'order_line_count_7d_avg': [300, 350],
    'order_line_count_global_avg': [290, 360],
    'return_count_7d_avg': [5, 8],
    'return_count_global_avg': [6, 7],
    'order_value_7d_avg': [4500.0, 5500.0],
    'order_value_global_avg': [4300.0, 5300.0],
    'return_value_7d_avg': [200.0, 320.0],
    'return_value_global_avg': [220.0, 300.0],
    'return_rate_value_7d_avg': [0.044, 0.058],
    'return_rate_value_global_avg': [0.051, 0.053],
    'review_rate_7d_avg': [0.05, 0.06],
    'review_rate_global_avg': [0.045, 0.055],
    'review_count_7d_avg': [6, 9],
    'review_count_global_avg': [5, 8],
    'vip_rate_7d_avg': [0.20, 0.25],
    'vip_rate_global_avg': [0.18, 0.22],
    'review_workflow_dist_7d_avg': ['{"wf1": 10, "wf2": 5}', '{"wf1": 12, "wf2": 7}'],
    'review_workflow_dist_global_avg': ['{"wf1": 110, "wf2": 50}', '{"wf1": 120, "wf2": 60}'],
}

df_metrics = pd.DataFrame(dummy_data)

# -----------------
# PDF Generation
# -----------------
class PDFReport(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 16)
        self.cell(0, 10, f'Live Monitoring Report - Merchant {self.merchant_id}', ln=True, align='C')
        self.ln(5)

    @property
    def epw(self):
        return self.w - 2 * self.l_margin  # Fix for missing epw attribute

    def add_metrics_table(self, df):
        self.set_font('Arial', 'B', 12)
        headers = [
            'Metric', '7-Day Avg', 'Global Avg'
        ]
        col_width = self.epw / len(headers)
        for header in headers:
            self.cell(col_width, 10, header, border=1)
        self.ln()

        self.set_font('Arial', '', 10)
        metrics = [
            ('Order Count', 'order_count_7d_avg', 'order_count_global_avg'),
            ('Order Line Count', 'order_line_count_7d_avg', 'order_line_count_global_avg'),
            ('Return Count', 'return_count_7d_avg', 'return_count_global_avg'),
            ('Order Value', 'order_value_7d_avg', 'order_value_global_avg'),
            ('Return Value', 'return_value_7d_avg', 'return_value_global_avg'),
            ('Return Rate (value)', 'return_rate_value_7d_avg', 'return_rate_value_global_avg'),
            ('Review Rate', 'review_rate_7d_avg', 'review_rate_global_avg'),
            ('Review Count', 'review_count_7d_avg', 'review_count_global_avg'),
            ('VIP Rate', 'vip_rate_7d_avg', 'vip_rate_global_avg'),
            ('Review Workflow Dist.', 'review_workflow_dist_7d_avg', 'review_workflow_dist_global_avg'),
        ]
        for label, col7, colg in metrics:
            self.cell(col_width, 8, label, border=1)
            self.cell(col_width, 8, str(df.iloc[0][col7]), border=1)
            self.cell(col_width, 8, str(df.iloc[0][colg]), border=1)
            self.ln()
        self.ln(5)


def generate_pdf_reports(df):
    pdf_paths = []
    for merchant_id, group in df.groupby('merchant_id'):
        pdf = PDFReport()
        pdf.merchant_id = merchant_id
        pdf.add_page()
        pdf.add_metrics_table(group.reset_index(drop=True))
        path = f'live_monitor_report_{merchant_id}.pdf'
        pdf.output(path)
        pdf_paths.append(path)
    return pdf_paths

# -----------------
# Email Report
# -----------------
def send_email_report(pdf_paths, recipients):
    for path in pdf_paths:
        msg = EmailMessage()
        msg['Subject'] = f"Live Monitoring Report - {os.path.basename(path)}"
        msg['From'] = os.getenv('EMAIL_USER')
        msg['To'] = ', '.join(recipients)
        msg.set_content('Please find the attached live monitoring report.')

    for path in pdf_paths:
        with open(path, 'rb') as f:
            file_data = f.read()
            file_name = os.path.basename(path)
            msg.add_attachment(file_data, maintype='application', subtype='pdf', filename=file_name)


    with smtplib.SMTP(os.getenv('SMTP_SERVER'), os.getenv('SMTP_PORT')) as smtp:
        smtp.starttls()
        smtp.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASSWORD'))
        smtp.send_message(msg)

def send_slack_dm(text,recipients):
    for email in recipients:
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

# -----------------
# Scheduled Job
# -----------------
def scheduled_job():
    pdfs = generate_pdf_reports(df_metrics)
    recipients = os.getenv('EMAIL_RECIPIENTS').split(',')
    send_email_report(pdfs, recipients)
    send_slack_dm(pdfs,recipients)

if __name__ == '__main__':
    # Run immediately once
    scheduled_job()
    # Then schedule future runs
    scheduler = BlockingScheduler()
    scheduler.add_job(scheduled_job, 'cron', hour=0, minute=0)  # daily at midnight UTC
    print('Scheduler started; ran immediately and scheduled daily runs.')
    scheduler.start()

