import csv
import random
import uuid
from faker import Faker
from datetime import datetime
import time
import os

faker = Faker()

ACTIONS = ['view', 'purchase']
PRODUCT_IDS = list(range(100, 200))
USER_IDS = list(range(10000, 10200))
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (Linux; Android 10)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1)"
]

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
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"events_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[
            "user_id", "session_id", "actions", "product_id", "price", "event_time", "user_agent"])
        writer.writeheader()
        for _ in range(num_events):
            writer.writerow(generate_event())
    print(f"Generated: {filepath}")

def main():
    output_dir = os.environ.get("OUTPUT_DIR", "/opt/generator/data/events")
    os.makedirs(output_dir, exist_ok=True)
    while True:
        generate_csv_file(output_dir, num_events=random.randint(10, 50))
        time.sleep(3)  # Wait 3 seconds before creating next file

if __name__ == "__main__":
    main()
