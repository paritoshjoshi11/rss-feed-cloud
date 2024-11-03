import os
import json
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load Confluent Cloud configuration from environment variables
kafka_server = os.environ.get('CLOUD_KAFKA_BOOTSTRAP_SERVERS')
api_key = os.environ.get('CLOUD_KAFKA_API_KEY')
api_secret = os.environ.get('CLOUD_KAFKA_API_SECRET')
topic = 'topic_0'  # Change to your topic name

# Check if any of the required values are None
if kafka_server is None or api_key is None or api_secret is None:
    raise ValueError("One or more environment variables are not set correctly.")

# Initialize the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username=api_key,
    sasl_plain_password=api_secret,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize to JSON
)

def push_sample_data():
    """Push sample data to Kafka topic."""
    sample_data = {
        'title': 'Sample Data Title',
        'description': 'This is a sample description for Kafka.',
        'link': 'https://example.com/sample-data',
        'timestamp': '2024-11-03T12:00:00Z'  # Example timestamp
    }
    
    # Send the sample data to Kafka
    producer.send(topic, value=sample_data)
    producer.flush()  # Ensure all messages are sent

    print(f"Sent sample data to topic '{topic}': {sample_data}")

if __name__ == "__main__":
    push_sample_data()
    producer.close()  # Close the producer when done
