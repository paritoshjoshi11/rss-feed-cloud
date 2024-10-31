import os
import feedparser
from kafka import KafkaProducer
import json
import logging
import time
from datetime import datetime,timedelta

SEEN_ITEMS_FILE = 'seen_items.json'

# Load seen items from file
def load_seen_items():
    if os.path.exists(SEEN_ITEMS_FILE):
        with open(SEEN_ITEMS_FILE, 'r') as f:
            content = f.read().strip()
            if content:  # Check if content is not empty
                return set(json.loads(content))  # Convert list to set
    return set()


# Save seen items to file
def save_seen_items(seen_items):
    with open(SEEN_ITEMS_FILE, 'w') as f:
        json.dump(list(seen_items), f)

def process_new_items(entries, seen_items):
    """Process new RSS feed entries."""
    new_items_found = False
    for entry in entries:
        if entry.id not in seen_items:  # Check if the item has been seen
            print(f"New item found: {entry.title}")
            print(f"Link: {entry.link}\n")
            seen_items.add(entry.id)  # Mark it as seen
            new_items_found = True
    return new_items_found

def fetch_rss_data(url):
    feed = feedparser.parse(url)

    if feed.bozo :
        print(f"Error fetching RSS feed: {feed.bozo_exception}")
    #     return
    # new_event_pubdate = datetime.strptime(feed.entries[0].published, date_format)

    # for entry in feed.entries :
    #     item = {
    #     'title': entry.title,
    #     'link': entry.link,
    #     'description': entry.description,
    #     'published': entry.published
    #     }
    #     item_pubdate = datetime.strptime(item['published'], date_format)
    #     if item_pubdate > last_event_pubdate:
    #         #send_to_kafka(topic,item,producer)
    #         print('Event sent:')
    #         print(item)
    #     else :
    #         print("No new events!!")
    for entry in feed.entries :
        print("-------------")
        print(entry)
    return feed.entries

# Main execution flow
if __name__ == "__main__":
    medium_url = 'https://medium.com/feed/tag/life'
    seen_items = load_seen_items()  # Load seen items at the start
    entries = fetch_rss_data(medium_url)  # Fetch the latest entries
    new_items_found = process_new_items(entries, seen_items)  # Process them
    if new_items_found:
        save_seen_items(seen_items)  # Save updates if new items were found
