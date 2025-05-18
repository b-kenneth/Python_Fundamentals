import csv
import random
import uuid
from faker import Faker
from datetime import datetime
import time
import os
import logging
import traceback

# Setup Faker
faker = Faker()

# Constants
ACTIONS = ['view', 'purchase']
PRODUCT_IDS = list(range(100, 200))
USER_IDS = list(range(10000, 10200))
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (Linux; Android 10)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1)"
]

# Setup Logging
LOG_FILE = os.environ.get("LOG_FILE", "/opt/generator/logs/generator.log")

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()  # Optional: keep console output
    ]
)

def generate_event():
    action = random.choice(ACTIONS)
    event = {
        "user_id": random.choice(USER_IDS),
        "session_id": str(uuid.uuid4()),
        "actions": action,
        "product_id": random.choice(PRODUCT_IDS),
        "price": round(random.uniform(5, 500), 2) if action == "purchase" else "",
        "event_time": datetime.utcnow().isoformat(),
        "user_agent": random.choice(USER_AGENTS)
    }
    return event

def generate_csv_file(output_dir, num_events=20):
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"events_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[
                "user_id", "session_id", "actions", "product_id", "price", "event_time", "user_agent"])
            writer.writeheader()
            for _ in range(num_events):
                writer.writerow(generate_event())
        logging.info(f"Generated: {filepath}")
    except Exception as e:
        logging.error("Error generating CSV file")
        logging.error(traceback.format_exc())

def main():
    try:
        output_dir = os.environ.get("OUTPUT_DIR", "/opt/generator/data/events")
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Starting data generator. Output dir: {output_dir}")
        while True:
            generate_csv_file(output_dir, num_events=random.randint(10, 50))
            time.sleep(5)
    except Exception as e:
        logging.critical("Fatal error in main loop")
        logging.critical(traceback.format_exc())

if __name__ == "__main__":
    main()

