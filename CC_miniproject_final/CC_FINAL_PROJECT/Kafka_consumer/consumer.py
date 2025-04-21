import mysql.connector
from kafka import KafkaConsumer
import json
import sys
import os
import random
from datetime import datetime

# MySQL Configuration
MYSQL_HOST = os.getenv('MYSQL_HOST', '172.20.144.1')
MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'sm@92004')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'logs_db')

# Kafka Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'api-logs')

# Error Codes and Messages Mapping
ERROR_CODES = {
    200: "Request succeeded",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    500: "Internal Server Error",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout"
}

# Initialize MySQL connection
def connect_mysql():
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        sys.exit(1)

# Process the Kafka message
def process_message(message):
    try:
        # Assume the message is in JSON format
        data = json.loads(message.value.decode('utf-8'))
        print(f"Consuming message: {data}")
        
        # Convert Unix timestamp to MySQL DATETIME format
        unix_ts = data.get("timestamp")
        if isinstance(unix_ts, (int, float)):
            dt_str = datetime.fromtimestamp(unix_ts).strftime('%Y-%m-%d %H:%M:%S')
        else:
            print(f"Invalid timestamp: {unix_ts}")
            return  # Skip this message if the timestamp is invalid
        
        # Connect to MySQL and insert the data
        connection = connect_mysql()
        cursor = connection.cursor()

        # Prepare the status field (convert 'success'/'failure' to HTTP status code)
        status = data.get("status", "unknown")
        
        if isinstance(status, str) and status.lower() in ['success', 'failure']:
            status = 200 if status.lower() == 'success' else 500
        elif isinstance(status, int) and status in ERROR_CODES:
            pass  # Status is a valid integer error code
        else:
            # Randomly choose a status code from the available ones if status is invalid
            status = random.choice(list(ERROR_CODES.keys()))

        # Handle error message (randomly assign an error code if not present)
        error_message = data.get("error")
        if not error_message:
            # Randomly choose an error message from the error codes mapping
            error_message = ERROR_CODES[status]

        # Prepare and execute the insert query
        insert_query = """
        INSERT INTO logs (timestamp, endpoint, status, response_time, service, error)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            dt_str,  # Use formatted timestamp
            data.get("endpoint"),
            status,
            data.get("response_time"),
            data.get("service"),
            error_message  # Ensure error field is populated
        ))

        # Commit changes and close connections
        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error processing message: {e}")
        # Handle errors here, log to MySQL with "error" field populated
        error_message = str(e)
        log_error(data, error_message)

# Run the Kafka consumer
def consume_messages():
    # Create a Kafka consumer instance
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id='python-consumer-group',
        auto_offset_reset='earliest'
    )
    
    print("Kafka Consumer started...")
    
    try:
        for msg in consumer:
            process_message(msg)
    except KeyboardInterrupt:
        print("Kafka Consumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()
