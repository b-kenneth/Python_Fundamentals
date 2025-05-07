from datetime import datetime

def format_timestamp(timestamp_str):
    """Format timestamp string to desired format"""
    dt = datetime.fromisoformat(timestamp_str)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def log_message(message, level="INFO"):
    """Log a message with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")
