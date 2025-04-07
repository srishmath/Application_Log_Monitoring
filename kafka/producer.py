from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

endpoints = ["/health", "/users", "/simulate", "/errors"]

while True:
    log = {
        "endpoint": random.choice(endpoints),
        "timestamp": time.time(),
        "status": random.choice(["success", "error"])
    }
    producer.send("api-logs", value=log)
    print("Log sent:", log)
    time.sleep(1)

