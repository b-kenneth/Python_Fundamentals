import random
import uuid
from datetime import datetime, timedelta

# Generator parameters
MIN_HEART_RATE = 55
MAX_HEART_RATE = 180

# Demographics for more realistic data
AGE_RANGES = [(18, 30), (31, 45), (46, 60), (61, 80)]
ACTIVITY_LEVELS = ["resting", "light_activity", "moderate_activity", "intense_activity", "sleeping"]

# Heart rate ranges by age and activity level
HEART_RATE_PROFILES = {
    (18, 30): {
        "resting": (60, 70, 5),        # (baseline_min, baseline_max, variation)
        "light_activity": (70, 90, 8),
        "moderate_activity": (90, 120, 12),
        "intense_activity": (120, 160, 15),
        "sleeping": (50, 60, 3)
    },
    (31, 45): {
        "resting": (65, 75, 5),
        "light_activity": (75, 95, 10),
        "moderate_activity": (95, 125, 12),
        "intense_activity": (125, 155, 15),
        "sleeping": (55, 65, 3)
    },
    (46, 60): {
        "resting": (68, 78, 6),
        "light_activity": (78, 98, 10),
        "moderate_activity": (98, 128, 15),
        "intense_activity": (120, 150, 18),
        "sleeping": (58, 68, 4)
    },
    (61, 80): {
        "resting": (70, 78, 7),
        "light_activity": (78, 100, 12),
        "moderate_activity": (95, 120, 15),
        "intense_activity": (110, 140, 20),
        "sleeping": (60, 70, 5)
    }
}

def create_customer_profiles(num_customers):
    """Create random customer profiles with realistic attributes"""
    customers = []
    
    for i in range(num_customers):
        # Generate a unique customer ID
        customer_id = f"C{str(uuid.uuid4())[:8]}"
        
        # Assign random age range and activity level
        age_range = random.choice(AGE_RANGES)
        activity = random.choice(ACTIVITY_LEVELS)
        
        # Get heart rate profile based on age and activity
        hr_profile = HEART_RATE_PROFILES[age_range][activity]
        baseline = random.randint(hr_profile[0], hr_profile[1])
        variation = hr_profile[2]
        
        # Create customer profile
        customer = {
            "customer_id": customer_id,
            "age_range": age_range,
            "activity": activity,
            "baseline": baseline,
            "variation": variation
        }
        customers.append(customer)
    
    return customers

def generate_heart_rate(baseline, variation):
    """Generate a realistic heart rate based on customer profile"""
    # Add normal distribution around the baseline with the given variation
    heart_rate = int(random.normalvariate(baseline, variation))
    # Ensure heart rate is within realistic bounds
    return max(MIN_HEART_RATE, min(heart_rate, MAX_HEART_RATE))

def generate_timestamp():
    """Generate a timestamp with a small random offset from current time"""
    now = datetime.now()
    # Add negative offset (0-3 seconds ago) to simulate recent readings
    offset = timedelta(seconds=random.uniform(0, 3))
    return (now - offset).isoformat()

def generate_heart_beat_data(customers):
    """Generate heart beat data for all customers"""
    heart_beat_records = []
    
    for customer in customers:
        # Occasionally change activity level to simulate daily patterns
        if random.random() < 0.05:  # 5% chance to change activity
            new_activity = random.choice(ACTIVITY_LEVELS)
            if new_activity != customer["activity"]:
                customer["activity"] = new_activity
                # Update baseline based on new activity
                hr_profile = HEART_RATE_PROFILES[customer["age_range"]][customer["activity"]]
                customer["baseline"] = random.randint(hr_profile[0], hr_profile[1])
                customer["variation"] = hr_profile[2]
                print(f"Customer {customer['customer_id']} activity changed to {new_activity}")
        
        heart_rate = generate_heart_rate(customer["baseline"], customer["variation"])
        record = {
            "customer_id": customer["customer_id"],
            "reading_time": generate_timestamp(),
            "heart_rate": heart_rate
        }
        heart_beat_records.append(record)
    
    return heart_beat_records
