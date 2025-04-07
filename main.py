from fastapi import FastAPI
import time
import random
import json
from kafka import KafkaProducer

app = FastAPI()

# --- API Endpoints ---

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/users")
def get_users():
    return {"users": ["alice", "bob"]}

@app.get("/products")
def get_products():
    return {"products": ["book", "laptop"]}

@app.get("/orders")
def get_orders():
    return {"orders": [1001, 1002]}

@app.post("/users")
def create_user(name: str):
    return {"message": f"User {name} created"}

@app.post("/products")
def create_product(name: str):
    return {"message": f"Product {name} created"}

@app.get("/status")
def get_status():
    return {"uptime": time.time()}

# --- Lazy Kafka Producer Setup ---

def get_producer():
    return KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

@app.get("/simulate")
def simulate_response():
    delay = random.uniform(0.1, 1.0)
    time.sleep(delay)
    log = {
        "endpoint": "/simulate",
        "delay": delay,
        "timestamp": time.time(),
        "status": "success"
    }
    try:
        producer = get_producer()
        producer.send("api-logs", value=log)
    except Exception as e:
        print(f"Kafka error: {e}")
    return {"delay": delay}

@app.get("/errors")
def error_simulation():
    if random.random() > 0.8:
        raise Exception("Random failure!")
    return {"status": "success"}

@app.get("/logs")
def get_logs():
    return {"log": "This is a sample log"}

