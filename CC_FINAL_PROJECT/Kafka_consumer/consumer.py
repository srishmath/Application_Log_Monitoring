# conn = mysql.connector.connect(
#     host="host.docker.internal",  # This points to your local system
#     user="root",
#     password="your_password",
#     database="logs_db"
# )
import mysql.connector
from kafka import KafkaConsumer
import json
import sys
import os

# MySQL Configuration
# MYSQL_HOST = os.getenv('MYSQL_HOST', '192.168.0.102')  # MySQL host (use host.docker.internal for Docker on Windows)
MYSQL_HOST = os.getenv('MYSQL_HOST', '10.30.203.186') 
MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')  # MySQL port
MYSQL_USER = os.getenv('MYSQL_USER', 'new_user')  # MySQL user
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'angel123')  # MySQL password
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'logs_db')  # Database to store the data

# Kafka Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')  # Kafka Broker
# KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'log_topic')  # Topic name
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'api-logs')  # Match with producer


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
        
        # Insert the data into MySQL
        connection = connect_mysql()
        cursor = connection.cursor()
        
        # # Adjust the query based on your data structure
        # insert_query = """INSERT INTO logs (log_id, log_message, timestamp) 
        #                   VALUES (%s, %s, %s)"""
        # cursor.execute(insert_query, (data['log_id'], data['log_message'], data['timestamp']))

        insert_query = """
        INSERT INTO logs (timestamp, endpoint, status, response_time, service, error)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            data.get("timestamp"),
            data.get("endpoint"),
            data.get("status"),
            data.get("response_time"),
            data.get("service"),
            data.get("error")
        ))

        
        # Commit changes
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error processing message: {e}")

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
