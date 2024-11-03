import os
import feedparser
from pymongo import MongoClient
import json
import logging
import time
from datetime import datetime, timedelta
import os
from kafka import KafkaProducer
from bson import ObjectId
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MongoDB connection details
uri = os.environ.get('MONGO_URI')
client = MongoClient(uri)
db = client['rss_feed']  # Connect to the database
collection = db['feed_life']  # Connect to the collection for RSS feed entries
seen_items_collection = db['seen_items']  # Connect to the collection for seen items


# Load seen items from MongoDB
def load_seen_items():
    seen_items = set()
    for document in seen_items_collection.find():
        seen_items.add(document['item_id'])  # Assuming 'item_id' is the field name in MongoDB
    return seen_items

# Save seen items to MongoDB
def save_seen_item(item_id):
    seen_items_collection.update_one({'item_id': item_id}, {'$setOnInsert': {'item_id': item_id}}, upsert=True)

def process_new_items(entries, seen_items):
    """Process new RSS feed entries and add to MongoDB."""
    new_items_found = False
    for entry in entries:
        if entry.id not in seen_items:  # Check if the item has been seen
            print(f"New item found: {entry.title}")
            print(f"Link: {entry.link}\n")
            seen_items.add(entry.id)  # Mark it as seen
            new_items_found = True

            # Prepare the document to insert into MongoDB
            document = {
                'title': entry.title,
                'link': entry.link,
                'description': entry.get('description', ''),
                'published': entry.get('published', ''),
                'published_parsed': entry.published_parsed  # Store parsed date if available
            }

            # Insert the document into the collection
            collection.insert_one(document)
            push_kafka(document)
            print("Inserted into MongoDB:", document)

            # Save the seen item to MongoDB
            save_seen_item(entry.id)

    return new_items_found

def fetch_rss_data(url):
    feed = feedparser.parse(url)

    if feed.bozo:
        print(f"Error fetching RSS feed: {feed.bozo_exception}")
        return []

    return feed.entries

# Custom JSON serializer to handle ObjectId serialization
def json_serializer(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def push_kafka(message):
    kafka_server = os.environ.get('CLOUD_KAFKA_BOOTSTRAP_SERVERS')
    api_key = os.environ.get('CLOUD_KAFKA_API_KEY')
    api_secret = os.environ.get('CLOUD_KAFKA_API_SECRET')
    topic = 'topic_0' 

    # Initialize the Kafka Producer
    producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username=api_key,
    sasl_plain_password=api_secret,
    value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('ascii'),
    key_serializer=lambda v: json.dumps(v).encode('ascii')
    )
    producer.send(topic, value=message)
    producer.flush()

# Main execution flow
if __name__ == "__main__":
    medium_url = 'https://medium.com/feed/tag/life'
    seen_items = load_seen_items()  # Load seen items from MongoDB at the start
    entries = fetch_rss_data(medium_url)  # Fetch the latest entries
    new_items_found = process_new_items(entries, seen_items)  # Process them
