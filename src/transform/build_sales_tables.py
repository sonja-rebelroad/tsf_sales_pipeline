# src/transform/build_sales_tables.py
import os, sys, json, glob, pandas as pd
from datetime import datetime, timedelta, UTC

# --- Always resolve imports the same way (VS Code, CLI, or Task Scheduler) ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.utils import logging as log

# -------------------------------------------------------------------
# 🔧 Ensure directories exist (important for GitHub Actions)
# -------------------------------------------------------------------
os.makedirs(os.path.join("data", "raw", "shopify"), exist_ok=True)
os.makedirs(os.path.join("data", "processed"), exist_ok=True)
os.makedirs(os.path.join("data", "reference"), exist_ok=True)

RAW_DIR  = os.path.join("data", "raw", "shopify")
PROC_DIR = os.path.join("data", "processed")
REF_DIR  = os.path.join("data", "reference")

# Desired output column order (A → AA)
FINAL_COLS = [
    "date", "channel", "order_id", "sku", "product_title", "variant_title",
    "week", "month", "quarter", "year",
    "units", "gross_revenue", "discounts", "shipping_charged", "refunds", "net_revenue",
    "region", "customer_type", "source", "promo_code", "promo_cost",
    "platform_fees", "COGS", "gross_margin",
    "country", "state", "sub_channel",
]

# ------------------------------------------------------------
# Reference mapping
# ------------------------------------------------------------
def load_channel_map():
    """Load Shopify → Channel + Sub-channel mapping."""
    p = os.path.join(REF_DIR, "channel_map.csv")
    chmap = pd.read_csv(p, comment="#").fillna("")
    if "sub_channel" not in chmap.columns:
        chmap["sub_channel"] = ""
    chmap = chmap[chmap["channel"].ne("") & chmap["sub_channel"].ne("")]
    return chmap

# ------------------------------------------------------------
# Merge raw JSON files (default: last 30 days, but resilient)
# ------------------------------------------------------------
def merge_all_orders(days_back: int = 30):
    """Combine Shopify JSON files from the last N days into one deduplicated list."""
    files = sorted(glob.glob(os.path.join(RAW_DIR, "orders_*.json")))
    if not files:
        raise SystemExit("No raw Shopify orders found.")

    cutoff = datetime.now(UTC) - timedelta(days=days_back)

    selected_files = []
    for f in files:
        ts_str = f.split("orders_")[-1].split(".json")[0]
        try:
            file_dt = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
            if file_dt >= cutoff:
                selected_files.append(f)
        except Exception:
            # If a filename doesn't parse, keep it (be permissive)
            selected_files.append(f)

    all_orders = []
    for f in selected_files:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                all_orders.extend(json.load(fh))
        except Exception as e:
            log.error(f"⚠️ Could not read {f}: {e}")

    # Deduplicate by Shopify order id
    seen, deduped = set(), []
    for o in all_orders:
        oid = o.get("id")
        if oid and oid not in seen:
            deduped.append(o)
            seen.add(oid)

    log.info(f"📦 Loaded {len(selected_files)} JSON files ({len(deduped)} unique orders).")
    return deduped

# ------------------------------------------------------------
# Geo helper
# ------------------------------------------------------------
def get_geo(o):
    shipping = o.get("shipping_address", {}) or {}
    billing  = o.get("billing_address",  {}) or {}
    state   = o.get("state")   or shipping.get("province_code") or billing.get("province_code")
    country = o.get("country") or shipping.get("country")       or billing.get("country")
    region  = o.get("region")

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

    if not region and state:
        region = STATE_TO_REGION.get((state or "").upper(), "Unknown")
    return region or "Unknown", country or "Unknown", (state or "Unknown").upper()

# ------------------------------------------------------------
# Normalize & aggregate
# ------------------------------------------------------------
def normalize(orders):
    """
    Normalize Shopify orders into:
      - line items (df)
      - aggregated sales at (date, channel, order_id, sku, ...)
    NOTE: We intentionally DROP the 'orders' metric to avoid SKU-level inflation.
    """
    rows = []
    for o in orders:
        order_id   = o.get("id")
        created_at = o.get("created_at")
        if not created_at:
            continue

        processed_at   = o.get("processed_at") or created_at
        cancel         = o.get("cancelled_at")
        landing_site   = o.get("landing_site") or ""
        source_name    = o.get("source_name")  or ""
        region, country, state = get_geo(o)
        discount_codes = ";".join(sorted(set(d.get("code", "") for d in o.get("discount_codes", []))))
        shipping_charged = sum(float(sl.get("price", 0)) for sl in o.get("shipping_lines", []))
        refunds_total    = sum(float(rf.get("transactions", [{}])[0].get("amount", 0)) for rf in o.get("refunds", []))
        customer = o.get("customer") or {}
        customer_type = "Repeat" if customer.get("orders_count", 0) > 1 else ("New" if customer else "Unknown")

        for li in o.get("line_items", []):
            rows.append({
                "order_id": order_id,
                "order_name": o.get("name"),
                "created_at": created_at,
                "processed_at": processed_at,
                "cancelled_at": cancel,
                "customer_id": customer.get("id"),
                "customer_type": customer_type,
                "source_name": source_name,
                "landing_site": landing_site,
                "discount_codes": discount_codes,
                "shipping_charged": shipping_charged,
                "refunds_total": float(refunds_total),
                "line_item_id": li.get("id"),
                "sku": li.get("sku") or "",
                "product_id": li.get("product_id"),
                "variant_id": li.get("variant_id"),
                "product_title": li.get("title"),
                "variant_title": li.get("variant_title"),
                "quantity": li.get("quantity", 0),
                "price": float(li.get("price", 0)),
                "discount_allocations": sum(float(a.get("amount", 0)) for a in li.get("discount_allocations", [])),
                "subtotal": float(li.get("price", 0)) * int(li.get("quantity", 0)),
                "region": region,
                "country": country,
                "state": state,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df, pd.DataFrame()

    # Channel mapping
    chmap = load_channel_map()
    df["source_key"] = df["source_name"].str.lower().fillna("")
    df = df.merge(chmap, how="left", on="source_key")
    df["channel"] = df["channel"].fillna("Shopify")
    df["sub_channel"] = df["sub_channel"].fillna("Online Store")

    # Time
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df = df.dropna(subset=["created_at"])
    df["created_at"] = df["created_at"].dt.tz_convert("America/New_York")
    df["date"]    = df["created_at"].dt.date
    df["week"]    = df["created_at"].dt.to_period("W").astype(str)
    df["month"]   = df["created_at"].dt.to_period("M").astype(str)
    df["quarter"] = df["created_at"].dt.to_period("Q").astype(str)
    df["year"]    = df["created_at"].dt.year

    # Metrics
    df["gross_revenue"] = df["subtotal"]
    df["discounts"]     = df["discount_allocations"]
    li_per_order = df.groupby("order_id")["order_id"].transform("count").replace(0, 1)
    df["net_revenue"] = (
        df["gross_revenue"]
        - df["discounts"]
        - df["refunds_total"] / li_per_order
    )

    # Aggregate BY ORDER + SKU (so order_id is explicit in the table)
    sales = (
        df.groupby(
            ["date","channel","order_id","sku","product_title","variant_title",
             "week","month","quarter","year","region","country","state","sub_channel"],
            dropna=False,
        )
        .agg(
            units=("quantity","sum"),
            gross_revenue=("gross_revenue","sum"),
            discounts=("discounts","sum"),
            shipping_charged=("shipping_charged","sum"),
            refunds=("refunds_total","sum"),
            net_revenue=("net_revenue","sum"),
            promo_code=("discount_codes", lambda x: ";".join(sorted(set(filter(None, x))))),
            customer_type=("customer_type", lambda x: "Mixed" if len(set(x)) > 1 else (list(set(x))[0] if len(set(x)) == 1 else "Unknown")),
        )
        .reset_index()
    )

    # Derived fields & final column order
    sales["source"] = sales["channel"] + " Direct"
    sales["promo_cost"] = 0.0
    sales["platform_fees"] = 0.0
    sales["COGS"] = 0.0
    sales["gross_margin"] = sales["net_revenue"]

    # Ensure exact column order (A–AA) and NO 'orders' column
    sales = sales[FINAL_COLS]

    return df, sales

# ------------------------------------------------------------
# Run transform
# ------------------------------------------------------------
def run_transform():
    """
    Build/update processed tables.
    If an existing CSV has an old schema (e.g., contains 'orders' or lacks 'order_id'),
    we write a fresh file to avoid schema conflicts.
    """
    os.makedirs(PROC_DIR, exist_ok=True)
    start = datetime.now()

    orders = merge_all_orders()
    df_li, new_sales = normalize(orders)
    if df_li.empty:
        log.info("⚠️ No line items found — skipping transform.")
        return

    sales_path = os.path.join(PROC_DIR, "sales_by_sku_channel_date.csv")

    write_fresh = True
    if os.path.exists(sales_path):
        try:
            existing = pd.read_csv(sales_path)
            if ("orders" in existing.columns) or ("order_id" not in existing.columns):
                # old schema—throw away and rebuild clean
                write_fresh = True
                log.info("🧹 Detected old schema in existing CSV (has 'orders' or missing 'order_id'). Rebuilding fresh.")
            else:
                # Same schema — keep only non-overlapping dates, then concat
                existing["date"] = pd.to_datetime(existing["date"]).dt.date
                new_min, new_max = new_sales["date"].min(), new_sales["date"].max()
                existing = existing[~existing["date"].between(new_min, new_max)]
                combined = pd.concat([existing[FINAL_COLS], new_sales[FINAL_COLS]], ignore_index=True)
                # For safety, drop dupes across the new key (date, channel, order_id, sku, sub_channel)
                combined = combined.drop_duplicates(
                    subset=["date","channel","order_id","sku","sub_channel"],
                    keep="last"
                )
                combined.to_csv(sales_path, index=False)
                write_fresh = False
                log.info(f"🧩 Merged {len(new_sales)} new rows with {len(existing)} existing rows.")
        except Exception as e:
            log.error(f"⚠️ Existing CSV could not be merged ({e}). Writing fresh.")
            write_fresh = True

    if write_fresh:
        new_sales[FINAL_COLS].to_csv(sales_path, index=False)
        log.info("📦 Wrote fresh processed sales file (schema reset).")

    # Line items table (always overwrite—it’s a point-in-time snapshot)
    df_li.to_csv(os.path.join(PROC_DIR, "shopify_line_items.csv"), index=False)

    log.info(f"✅ Updated processed tables at {PROC_DIR}/")
    log.info(f"⏱️ Transform duration: {datetime.now() - start}")


if __name__ == "__main__":
    run_transform()
