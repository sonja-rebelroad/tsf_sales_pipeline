import os
import sys
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import gspread
from gspread_dataframe import set_with_dataframe

# --- Force consistent import path for any runtime environment ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils import logging as log


# -------------------------------------------------------------------
# 🌍 Load environment variables
# -------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # -> TSF_SALES_PIPELINE/src
PROJECT_ROOT = os.path.dirname(BASE_DIR)                                # -> TSF_SALES_PIPELINE
ENV_PATH = os.path.join(PROJECT_ROOT, "config", ".env")

# Load .env if available (for local)
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    log.info("🌐 No local .env found — using environment variables (e.g., GitHub secrets).")

# --- Read config values ---
SA_JSON_PATH = os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON", "")
WB_NAME = os.getenv("GOOGLE_SHEETS_WORKBOOK_NAME", "TSF Sales & KPIs")

# -------------------------------------------------------------------
# 🧭 Sheet helpers
# -------------------------------------------------------------------
def open_sheet():
    """Authenticate using the Google service account and open or create the workbook."""
    if SA_JSON_PATH and os.path.exists(SA_JSON_PATH):
        log.info(f"🔐 Using service account JSON: {SA_JSON_PATH}")
        gc = gspread.service_account(filename=SA_JSON_PATH)
    elif os.getenv("GOOGLE_SERVICE_ACCOUNT"):
        # For GitHub Actions: service account JSON stored in a secret
        from google.oauth2.service_account import Credentials
        import json
        creds_dict = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT"))
        creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        log.info("🔐 Using service account credentials from GitHub Secrets.")
    else:
        raise FileNotFoundError("❌ No service account credentials found.")

    try:
        sh = gc.open(WB_NAME)
        log.info(f"📘 Connected to workbook: {WB_NAME}")
    except gspread.SpreadsheetNotFound:
        sh = gc.create(WB_NAME)
        log.info(f"🆕 Created new workbook: {WB_NAME}")
    return sh


def upsert(sh, title, df):
    """Write DataFrame to worksheet with timestamp and header spacing."""
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=5, cols=5)

    ws.clear()

    timestamp = datetime.now().strftime("Last updated %m/%d/%y %H:%M:%S")
    ws.update("A1", [[timestamp]])

    set_with_dataframe(ws, df, row=3, include_column_header=True, include_index=False)
    log.info(f"✅ Published {len(df)} rows × {len(df.columns)} cols to worksheet '{title}'")


# -------------------------------------------------------------------
# 🚀 Main loader
# -------------------------------------------------------------------
def run_load():
    """Push processed CSVs from data/processed/ to Google Sheets."""
    sh = open_sheet()

    sales_p = os.path.join(PROJECT_ROOT, "data", "processed", "sales_by_sku_channel_date.csv")
    li_p = os.path.join(PROJECT_ROOT, "data", "processed", "shopify_line_items.csv")

    if os.path.exists(sales_p):
        df = pd.read_csv(sales_p)
        log.info(f"📊 Uploading {len(df)} rows from {sales_p}")
        upsert(sh, "Sales_By_SKU_Channel_Date", df)
    else:
        log.error(f"❌ Missing file: {sales_p}")

    if os.path.exists(li_p):
        li = pd.read_csv(li_p)
        log.info(f"📊 Uploading {len(li)} rows from {li_p}")
        upsert(sh, "Shopify_Line_Items", li)
    else:
        log.error(f"❌ Missing file: {li_p}")

    log.info("✨ Finished publishing to Google Sheets.")


# -------------------------------------------------------------------
# 🧪 Debug mode (manual run)
# -------------------------------------------------------------------
if __name__ == "__main__":
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")
    print(f".env path: {ENV_PATH}")
    print(f"Service account: {SA_JSON_PATH or 'Loaded from GitHub Secrets'}")
    print(f"Workbook name: {WB_NAME}")
    run_load()
