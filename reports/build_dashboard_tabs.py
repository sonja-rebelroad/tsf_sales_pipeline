import os
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime

from dotenv import load_dotenv
from pathlib import Path

# Resolve the path relative to the project root
root_dir = Path(__file__).resolve().parent.parent
load_dotenv(root_dir / "config" / ".env")


SA_JSON = os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON", "./config/service_account.json")
WB_NAME = os.getenv("GOOGLE_SHEETS_WORKBOOK_NAME", "TSF Sales & KPIs")

def open_sheet():
    gc = gspread.service_account(filename=SA_JSON)
    try:
        sh = gc.open(WB_NAME)
    except gspread.SpreadsheetNotFound:
        raise SystemExit("Google Sheet not found. Make sure you've created and shared it with the service account.")
    return sh

def kpi_summary(df):
    """Compute top-level KPIs for most recent and cumulative periods"""
    df["date"] = pd.to_datetime(df["date"])
    today = datetime.now().date()
    df_yesterday = df[df["date"] == (today - pd.Timedelta(days=1))]
    df_mtd = df[df["date"].dt.month == today.month]

    kpis = {
        "Metric": ["Orders", "Gross Revenue", "Discounts", "Net Revenue"],
        "Yesterday": [
            df_yesterday["orders"].sum(),
            df_yesterday["gross_revenue"].sum(),
            df_yesterday["discounts"].sum(),
            df_yesterday["net_revenue"].sum(),
        ],
        "Month-to-Date": [
            df_mtd["orders"].sum(),
            df_mtd["gross_revenue"].sum(),
            df_mtd["discounts"].sum(),
            df_mtd["net_revenue"].sum(),
        ],
        "Year-to-Date": [
            df[df["date"].dt.year == today.year]["orders"].sum(),
            df[df["date"].dt.year == today.year]["gross_revenue"].sum(),
            df[df["date"].dt.year == today.year]["discounts"].sum(),
            df[df["date"].dt.year == today.year]["net_revenue"].sum(),
        ],
    }
    return pd.DataFrame(kpis)

def channel_summary(df):
    """Summarize revenue by channel"""
    return (
        df.groupby("channel", dropna=False)
        .agg(
            Orders=("orders", "sum"),
            Gross_Revenue=("gross_revenue", "sum"),
            Discounts=("discounts", "sum"),
            Net_Revenue=("net_revenue", "sum"),
        )
        .reset_index()
    )

def build_tabs():
    sh = open_sheet()

    df = pd.read_csv("data/processed/sales_by_sku_channel_date.csv")
    kpi_df = kpi_summary(df)
    ch_df = channel_summary(df)

    # --- DAILY SUMMARY TAB ---
    try:
        ws1 = sh.worksheet("Daily_Summary")
        ws1.clear()
    except gspread.WorksheetNotFound:
        ws1 = sh.add_worksheet(title="Daily_Summary", rows=100, cols=20)
    set_with_dataframe(ws1, kpi_df, include_index=False, include_column_header=True, resize=True)
    ws1.update("A1", "🧭 DAILY KPI SUMMARY (auto-generated)")
    ws1.format("A1", {"textFormat": {"bold": True, "fontSize": 12}})

    # --- CUMULATIVE DASHBOARD TAB ---
    try:
        ws2 = sh.worksheet("Cumulative_Dashboard")
        ws2.clear()
    except gspread.WorksheetNotFound:
        ws2 = sh.add_worksheet(title="Cumulative_Dashboard", rows=100, cols=20)
    set_with_dataframe(ws2, ch_df, include_index=False, include_column_header=True, resize=True)
    ws2.update("A1", "📊 CUMULATIVE REVENUE BY CHANNEL (auto-generated)")
    ws2.format("A1", {"textFormat": {"bold": True, "fontSize": 12}})

    print("✅ Dashboard tabs created and populated in Google Sheets.")

if __name__ == "__main__":
    build_tabs()
