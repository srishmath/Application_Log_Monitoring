from fastapi import FastAPI, HTTPException
import time
import random
import json
import logging
from kafka import KafkaProducer
from pydantic import BaseModel

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Models ---
class UserCreate(BaseModel):
    name: str

class ProductCreate(BaseModel):
    name: str

# --- Kafka Producer Setup ---
def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=['kafka:9092', 'localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=50,  # Wait up to 50ms to batch messages
            batch_size=16384,  # 16KB batch size
            compression_type='gzip',  # Compress messages
            retries=1,  # Faster failover
            acks=1  # Faster than 'all' but less reliable
        )
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None

# --- Helper Functions ---
async def send_to_kafka(topic: str, data: dict):
    producer = get_producer()
    if producer:
        try:
            future = producer.send(topic, value=data)
            # Don't wait for flush unless critical
            # producer.flush(timeout=0.1)  # Optional: partial blocking
        except Exception as e:
            logger.error(f"Kafka send failed: {e}")

def generate_log(endpoint: str, status: str, response_time: float = None, error: str = None):
    return {
        "timestamp": time.time(),
        "endpoint": endpoint,
        "status": status,
        "response_time": response_time or random.uniform(0.1, 1.0),
        "error": error,
        "service": "api-server"
    }

# --- API Endpoints ---
# @app.get("/health")
# def health_check():
#     log = generate_log("/health", "success")
#     send_to_kafka("api-logs", log)
#     return {"status": "healthy"}

# Health check - minimal processing
@app.get("/health")
async def health_check():
    return {"status": "healthy"}  # Skip Kafka for health checks

# High-frequency endpoints
@app.get("/status")
async def get_status():
    log = generate_log("/status", "success")
    asyncio.create_task(send_to_kafka("api-logs", log))
    return {"status": "running", "uptime": time.time()}

@app.get("/users")
def get_users():
    start_time = time.time()
    users = ["alice", "bob", "charlie"]
    response_time = time.time() - start_time
    
    log = generate_log("/users", "success", response_time)
    send_to_kafka("api-logs", log)
    return {"users": users}

@app.get("/products")
def get_products():
    start_time = time.time()
    products = ["laptop", "phone", "tablet"]
    response_time = time.time() - start_time
    
    log = generate_log("/products", "success", response_time)
    send_to_kafka("api-logs", log)
    return {"products": products}

@app.get("/orders")
def get_orders():
    start_time = time.time()
    orders = [{"id": 1001, "item": "laptop"}, {"id": 1002, "item": "phone"}]
    response_time = time.time() - start_time
    
    log = generate_log("/orders", "success", response_time)
    send_to_kafka("api-logs", log)
    return {"orders": orders}

@app.post("/users")
def create_user(user: UserCreate):
    start_time = time.time()
    # Simulate DB write delay
    time.sleep(random.uniform(0.01, 0.05))
    response_time = time.time() - start_time
    
    log = generate_log("/users", "success", response_time)
    send_to_kafka("api-logs", log)
    return {"message": f"User {user.name} created"}

@app.post("/products")
def create_product(product: ProductCreate):
    start_time = time.time()
    # Simulate DB write delay
    time.sleep(random.uniform(0.01, 0.05))
    response_time = time.time() - start_time
    
    log = generate_log("/products", "success", response_time)
    send_to_kafka("api-logs", log)
    return {"message": f"Product {product.name} created"}

# @app.get("/status")
# def get_status():
#     log = generate_log("/status", "success")
#     send_to_kafka("api-logs", log)
#     return {"status": "running", "uptime": time.time()}

@app.get("/simulate")
def simulate_response():
    start_time = time.time()
    delay = random.uniform(0.01, 0.1)
    time.sleep(delay)
    response_time = time.time() - start_time
    
    log = generate_log("/simulate", "success", response_time)
    send_to_kafka("api-logs", log)
    return {"delay": delay}

@app.get("/errors")
def error_simulation():
    try:
        if random.random() > 0.8:
            raise HTTPException(status_code=500, detail="Random failure!")
        
        log = generate_log("/errors", "success")
        send_to_kafka("api-logs", log)
        return {"status": "success"}
    except Exception as e:
        log = generate_log("/errors", "error", error=str(e))
        send_to_kafka("api-logs", log)
        raise

@app.get("/logs")
def get_logs():
    log = generate_log("/logs", "success")
    send_to_kafka("api-logs", log)
    return {"logs": ["sample log 1", "sample log 2"]}