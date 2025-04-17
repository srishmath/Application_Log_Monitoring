import json
import time
import random
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError
from kafka.admin import NewTopic

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_topic():
    try:
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        topic_list = [NewTopic(name="api-logs", num_partitions=3, replication_factor=1)]
        admin.create_topics(new_topics=topic_list, validate_only=False)
        logger.info("Topic 'api-logs' created successfully")
    except Exception as e:
        logger.warning(f"Topic creation failed: {e}")

def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            acks='all'
        )
    except Exception as e:
        logger.error(f"Producer creation failed: {e}")
        return None

def generate_log():
    endpoints = [
        "/health", "/users", "/products", "/orders",
        "/status", "/simulate", "/errors", "/logs"
    ]
    statuses = ["success"] * 8 + ["error"] * 2  # 80% success, 20% error
    
    return {
        "timestamp": time.time(),
        "endpoint": random.choice(endpoints),
        "status": random.choice(statuses),
        "response_time": random.uniform(0.1, 2.0),
        "service": "log-generator"
    }

if __name__ == "__main__":
    create_topic()
    producer = get_producer()
    
    if producer:
        while True:
            log = generate_log()
            try:
                producer.send("api-logs", value=log)
                logger.info(f"Produced log: {log}")
            except KafkaError as e:
                logger.error(f"Failed to send message: {e}")
            
            time.sleep(random.uniform(0.1, 1.0))