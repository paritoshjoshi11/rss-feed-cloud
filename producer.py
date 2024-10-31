import feedparser
from kafka import KafkaProducer
import json
import logging
import time
from datetime import datetime,timedelta

def serializer(message):
    return json.dumps(message).encode('utf-8')

def send_to_kafka(topic,item,producer) :
    item_json = json.dumps(item)
    producer.send(topic, value=item_json)
    producer.flush()

def fetch_rss_data(url,date_format,last_event_pubdate,topic,producer):
    feed = feedparser.parse(url)

    if feed.bozo :
        print(f"Error fetching RSS feed: {feed.bozo_exception}")
        return
    new_event_pubdate = datetime.strptime(feed.entries[0].published, date_format)

    for entry in feed.entries :
        item = {
        'title': entry.title,
        'link': entry.link,
        'description': entry.description,
        'published': entry.published
        }
        item_pubdate = datetime.strptime(item['published'], date_format)
        if item_pubdate > last_event_pubdate:
            #send_to_kafka(topic,item,producer)
            print('Event sent:')
            print(item)
        else :
            print("No new events!!")
        return new_event_pubdate


def main():
    medium_url = 'https://medium.com/feed/tag/life'
    topic='rss_feed_topic'
    # Starting parsing from current date midnight
    DATE_FORMAT = "%a, %d %b %Y %H:%M:%S %Z"
    last_event_pubdate = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=serializer
        )
    while True :
        last_event_pubdate = fetch_rss_data(medium_url,DATE_FORMAT,last_event_pubdate,topic,producer)
        time.sleep(60)



if __name__ == "__main__" :
    logging.basicConfig(level="DEBUG")
    try :
        main()
    except KeyboardInterrupt :
        pass    