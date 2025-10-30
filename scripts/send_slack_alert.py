# File: scripts/send_slack_alert.py
import os
import sys
import requests
from dotenv import load_dotenv

# Load env
dotenv_path = os.path.join(os.path.dirname(__file__), "../.env")  # adjust if needed
load_dotenv(dotenv_path)

webhook_url = os.getenv("SLACK_WEBHOOK_URL")

def send_slack_alert(message):
    if not webhook_url:
        print("⚠️ No Slack webhook URL found in .env")
        return

    payload = {"text": message}
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Slack alert sent.")
    except Exception as e:
        print(f"❌ Slack alert failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        message = " ".join(sys.argv[1:])
        send_slack_alert(message)
    else:
        print("⚠️ No message provided.")
