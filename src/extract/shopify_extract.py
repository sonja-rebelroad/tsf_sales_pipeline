import os
import json
import time
from datetime import datetime, timedelta
import requests
from urllib.parse import urlencode
from dotenv import load_dotenv
from utils import logging as log

# -------------------------------------------------------------------
# 🔧 Load environment and constants
# -------------------------------------------------------------------
load_dotenv(os.path.join("config", ".env"))

SHOP = os.getenv("SHOPIFY_STORE_DOMAIN")
TOKEN = os.getenv("SHOPIFY_API_TOKEN")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")

RAW_DIR = os.path.join("data", "raw", "shopify")
os.makedirs(RAW_DIR, exist_ok=True)

# -------------------------------------------------------------------
# 🧩 Fetch orders from Shopify REST API
# -------------------------------------------------------------------
def fetch_orders(since=None, until=None, status="any", limit=250):
    """Fetch all orders from Shopify between given timestamps."""
    assert SHOP and TOKEN, "Missing SHOPIFY credentials in .env"

    base = f"https://{SHOP}/admin/api/{API_VERSION}/orders.json"
    params = {
        "status": status,
        "limit": limit,
        "created_at_min": since,
        "created_at_max": until,
    }
    headers = {"X-Shopify-Access-Token": TOKEN}

    url = f"{base}?{urlencode({k: v for k, v in params.items() if v})}"
    orders = []

    while True:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        payload = r.json()
        batch = payload.get("orders", [])
        orders.extend(batch)

        # Pagination handling
        link = r.headers.get("Link", "")
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<") + 1 : part.find(">")]
        if not next_url:
            break
        url = next_url
        time.sleep(0.25)

    log.info(f"✅ Retrieved {len(orders)} orders from Shopify.")
    return orders


# -------------------------------------------------------------------
# 🌍 Region enrichment
# -------------------------------------------------------------------
STATE_TO_REGION = {
    "CT":"Northeast","ME":"Northeast","MA":"Northeast","NH":"Northeast",
    "NJ":"Northeast","NY":"Northeast","PA":"Northeast","RI":"Northeast","VT":"Northeast",
    "IL":"Midwest","IN":"Midwest","IA":"Midwest","KS":"Midwest","MI":"Midwest",
    "MN":"Midwest","MO":"Midwest","NE":"Midwest","ND":"Midwest","OH":"Midwest",
    "SD":"Midwest","WI":"Midwest",
    "AL":"South","AR":"South","DE":"South","FL":"South","GA":"South","KY":"South",
    "LA":"South","MD":"South","MS":"South","NC":"South","OK":"South","SC":"South",
    "TN":"South","TX":"South","VA":"South","WV":"South",
    "AK":"West","AZ":"West","CA":"West","CO":"West","HI":"West","ID":"West",
    "MT":"West","NV":"West","NM":"West","OR":"West","UT":"West","WA":"West","WY":"West"
}

def enrich_orders_with_region(orders):
    """Adds state, region, and country fields to each order."""
    for o in orders:
        shipping = o.get("shipping_address", {}) or {}
        billing = o.get("billing_address", {}) or {}
        province_code = (
            (shipping.get("province_code") or billing.get("province_code") or "")
        ).upper()
        region = STATE_TO_REGION.get(province_code, province_code or "Unknown")
        country = (
            shipping.get("country")
            or billing.get("country")
            or "United States"
        )
        o["state"] = province_code or "Unknown"
        o["region"] = region
        o["country"] = country
    return orders


# -------------------------------------------------------------------
# 🧹 Cleanup helper
# -------------------------------------------------------------------
def cleanup_raw_files(keep_days=90):
    """Delete raw Shopify JSONs older than N days."""
    cutoff = datetime.utcnow() - timedelta(days=keep_days)
    for f in os.listdir(RAW_DIR):
        if f.startswith("orders_") and f.endswith(".json"):
            ts = f.replace("orders_", "").replace(".json", "")
            try:
                file_dt = datetime.strptime(ts, "%Y%m%dT%H%M%SZ")
                if file_dt < cutoff:
                    os.remove(os.path.join(RAW_DIR, f))
                    log.info(f"🧹 Removed old raw file {f}")
            except Exception:
                continue


# -------------------------------------------------------------------
# 🚀 Pipeline entrypoint
# -------------------------------------------------------------------
def run_extract(days: int = 7):
    """Fetch and enrich last N days of Shopify orders and save to /data/raw/shopify."""
    since_dt = datetime.utcnow() - timedelta(days=days)
    until_dt = datetime.utcnow()
    since = since_dt.strftime("%Y-%m-%dT%H:%M:%S-00:00")
    until = until_dt.strftime("%Y-%m-%dT%H:%M:%S-00:00")

    orders = fetch_orders(since=since, until=until)
    if not orders:
        log.info("⚠️ No new orders found — skipping file creation.")
        return

    orders = enrich_orders_with_region(orders)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(RAW_DIR, f"orders_{timestamp}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(orders, f, ensure_ascii=False, indent=2)

    log.info(f"💾 Wrote {len(orders)} enriched orders to {out_path}")
    cleanup_raw_files()


# -------------------------------------------------------------------
# 🧩 CLI support (manual run)
# -------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7, help="Days of history to pull")
    args = ap.parse_args()
    run_extract(days=args.days)
