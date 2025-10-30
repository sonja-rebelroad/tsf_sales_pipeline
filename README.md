# Taking Stock Foods — Sales Pipeline Scaffold

This repo scaffolds a data pipeline to pull Shopify orders, normalize sales metrics,
and publish a daily summary + cumulative dashboard (Google Sheets). Amazon & TikTok
connectors are stubbed for later.

## Structure
- `config/`: `.env` and service account JSON for Google Sheets
- `data/raw/`: raw API payloads (JSON/JSONL) by source
- `data/processed/`: normalized CSV/Parquet facts & dims
- `data/reference/`: mapping tables (channels, SKUs, promos, influencers)
- `src/`: Python modules
  - `extract/`: source connectors
  - `transform/`: pandas transforms to facts/dims
  - `load/`: Google Sheets publisher
  - `utils/`: logging, helpers
- `reports/specs/`: dashboard specs and mockups
- `notebooks/exploratory/`: ad hoc analysis
- `tests/`: unit tests for transforms

## Quickstart
1. Create & activate a virtual environment.
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. Copy `config/.env.example` to `config/.env` and fill in values.
3. Put a Google service account JSON at `config/service_account.json` (or set an absolute path in `.env`).
4. Test Shopify connectivity:
   ```bash
   python src/extract/shopify_test.py --since 2025-10-01
   ```
5. Build normalized tables:
   ```bash
   python src/transform/build_sales_tables.py
   ```
6. Publish to Google Sheets:
   ```bash
   python src/load/publish_to_sheets.py
   ```

## Metrics & Definitions (initial draft)
- **Gross Revenue**: sum of line item extended prices before discounts & refunds.
- **Discounts**: price reductions from discount codes, automatic discounts, BOGOs; captured at both order- and line-level.
- **Net Revenue**: Gross - Discounts - Refunds + Shipping Charged (optional, if considered topline) - Taxes (optional).
- **By Channel**: derived from Shopify `source_name`, `app_id`, and `landing_site` parsing; explicit map in `data/reference/channel_map.csv`.
- **By SKU**: explode order line items, use `variant_sku`; map missing SKUs via `sku_map.csv`.
- **Time Buckets**: day/week/month/quarter/year from `created_at` in store timezone.
- **Shipping Cost Tracking**: start with `shipping_lines.price` charged to customer. For actual carrier cost, reconcile later with QBO.
- **Promo Cost**: table in `promo_budget.csv` as % of sales by promo; influencer fees in `influencer_map.csv`.

## Dashboards
- **Daily Summary**: yesterday & MTD snapshots for revenue, orders, AOV, discounts, top SKUs, active promos.
- **Cumulative Dashboard**: trendlines, YoY, promo performance, channel mix, progress vs KPIs (e.g., BFCM $10k–$35k).

## Notes
- Amazon & TikTok connectors are placeholders; transforms are source-agnostic once data is normalized.
