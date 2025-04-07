import requests
import random
import time

endpoints = [
    "/health", "/users", "/products", "/orders",
    "/status", "/simulate", "/errors", "/logs"
]

while True:
    endpoint = random.choice(endpoints)
    try:
        response = requests.get(f"http://localhost:8000{endpoint}")
        print(f"{endpoint} → {response.status_code}")
    except Exception as e:
        print(f"Request failed: {e}")
    time.sleep(random.uniform(0.5, 1.5))

