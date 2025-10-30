# run_pipeline.py
import os, sys, subprocess, time, traceback
from datetime import datetime
from pathlib import Path

# --- Locate project root ---
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# --- Ensure src is on sys.path ---
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils import logging as log

# --- Logging setup ---
def log_msg(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_DIR / "scheduler_log.txt", "a", encoding="utf-8") as f:
        f.write(line + "\n")

# --- Optional delay to allow Drive to mount ---
log_msg("🕓 Waiting 20 seconds for Drive mount...")
time.sleep(20)

# --- Detect virtual environment ---
venv_python = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
python_exec = str(venv_python if venv_python.exists() else sys.executable)
log_msg(f"🐍 Using Python interpreter: {python_exec}")

# --- Slack alert helper ---
def notify_slack(text):
    try:
        from dotenv import load_dotenv
        import requests
        load_dotenv(PROJECT_ROOT / "config" / ".env")
        url = os.getenv("SLACK_WEBHOOK_URL")
        if not url:
            log_msg("⚠️ No SLACK_WEBHOOK_URL found — skipping Slack alert.")
            return
        requests.post(url, json={"text": text}, timeout=10)
        log_msg("✅ Slack notification sent.")
    except Exception as e:
        log_msg(f"⚠️ Slack alert failed: {e}")

# --- Run a stage ---
def run_stage(module, func):
    import importlib
    import src.utils.logging as log
    log_msg(f"🚀 Starting stage: {module}.{func}")
    try:
        m = importlib.import_module(module)
        getattr(m, func)()
        log_msg(f"✅ Stage complete: {module}")
        return True
    except Exception as e:
        tb = traceback.format_exc()
        log_msg(f"❌ Stage failed: {module} — {e}")
        log_msg(tb)
        notify_slack(f"❌ TSF Sales Pipeline FAILED\n• Stage: {module}\n• Error: {e}\n```\n{tb[-4000:]}\n```")
        return False

def main():
    start = datetime.now()
    log_msg(f"🧙 TSF Sales Pipeline started at {start}")
    log_msg(f"📂 Current working directory: {os.getcwd()}")

    stages = [
        ("src.extract.shopify_extract", "run_extract"),
        ("src.transform.build_sales_tables", "run_transform"),
        ("src.load.publish_to_sheets", "run_load"),
    ]

    success = True
    for module, func in stages:
        if not run_stage(module, func):
            success = False
            break

    end = datetime.now()
    elapsed = end - start
    msg = f"✨ Pipeline finished at {end} (duration: {elapsed})"
    log_msg(msg)
    if success:
        notify_slack(f"✅ TSF Sales Pipeline completed successfully in {elapsed}.")

if __name__ == "__main__":
    main()
