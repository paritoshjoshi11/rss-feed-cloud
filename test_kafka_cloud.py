import json
from kafka import KafkaProducer
folderName = "./kafkaCerts/"
producer = KafkaProducer(
    bootstrap_servers="kafka-rss-feed-paritoshjoshi11-ebfb.b.aivencloud.com:24695",
    security_protocol="SSL",
    ssl_cafile=folderName+"ca.pem",
    ssl_certfile=folderName+"service.cert",
    ssl_keyfile=folderName+"service.key",
    value_serializer=lambda v: json.dumps(v).encode('ascii'),
    key_serializer=lambda v: json.dumps(v).encode('ascii')
    )

producer.send("rss-feed",key={"key":1},value={"message":"Hello kafka"})
producer.flush()