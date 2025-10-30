import logging
import os
from datetime import datetime

# Directory for logs (one per day)
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "reports")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y%m%d')}.log")

# Configure logger once
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ✅ The functions your other modules call
def info(msg):
    logging.info(msg)

def error(msg):
    logging.error(msg)
