import requests
import random
import time
import logging
import concurrent.futures
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "http://localhost:8000"
ENDPOINTS = [
    "/health", "/users", "/products", "/orders",
    "/status", "/simulate", "/errors", "/logs"
]
CONCURRENT_WORKERS = 5  # Number of parallel requests
MIN_DELAY = 0.05       # 50ms minimum between requests
MAX_DELAY = 0.5        # 500ms maximum between requests
TIMEOUT = 2.0          # 2 seconds max per request

def make_request(endpoint):
    """Make a single API request with timing and error handling"""
    start_time = time.time()
    try:
        if endpoint in ["/users", "/products"] and random.random() > 0.7:
            data = {"name": f"user_{random.randint(1, 100)}"}
            response = requests.post(
                f"{BASE_URL}{endpoint}",
                json=data,
                timeout=TIMEOUT
            )
        else:
            response = requests.get(
                f"{BASE_URL}{endpoint}",
                timeout=TIMEOUT
            )
        
        response_time = time.time() - start_time
        logger.info(
            f"{endpoint.ljust(10)} | "
            f"Status: {response.status_code} | "
            f"Time: {response_time:.3f}s"
        )
    except Exception as e:
        logger.error(f"Request to {endpoint} failed: {str(e)}")

def simulate_traffic():
    """Generate concurrent API traffic with controlled rate"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        while True:
            endpoint = random.choice(ENDPOINTS)
            executor.submit(make_request, endpoint)
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

if __name__ == "__main__":
    logger.info("Starting optimized API traffic simulator...")
    logger.info(f"Configuration: {CONCURRENT_WORKERS} workers, "
               f"delay {MIN_DELAY}-{MAX_DELAY}s, timeout {TIMEOUT}s")
    simulate_traffic()