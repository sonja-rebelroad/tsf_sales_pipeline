import os, sys
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

if not os.path.exists(ENV_PATH):
    raise FileNotFoundError(f"❌ .env file not found at: {ENV_PATH}")

load_dotenv(ENV_PATH)

SA_JSON = os.getenv(
    "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON",
    os.path.join(PROJECT_ROOT, "config", "service_account.json"),
)
WB_NAME = os.getenv("GOOGLE_SHEETS_WORKBOOK_NAME", "TSF Sales & KPIs")

# -------------------------------------------------------------------
# 🧭 Sheet helpers
# -------------------------------------------------------------------
def open_sheet():
    """Authenticate using the Google service account and open or create the workbook."""
    if not os.path.exists(SA_JSON):
        raise FileNotFoundError(f"❌ Service account file not found: {SA_JSON}")

    log.info(f"🔐 Using service account JSON: {SA_JSON}")
    gc = gspread.service_account(filename=SA_JSON)

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
    print(f"Service account JSON: {SA_JSON}")
    print(f"Workbook name: {WB_NAME}")
    run_load()
