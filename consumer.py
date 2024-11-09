import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql.functions import from_json, udf
import google.generativeai as genai
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()



kafka_config = os.environ.get('SPARK_KAFKA_CONFIG')
kafka_server = os.environ.get('SPARK_KAFKA_CLUSTER')
mongodb_uri= os.environ.get('NONGO_URI')

# Initialize Spark session with MongoDB connector
spark = SparkSession.builder \
    .appName("MongoDB Write Example") \
    .config("spark.mongodb.input.uri", mongodb_uri) \
    .config("spark.mongodb.output.uri", mongodb_uri) \
    .getOrCreate()

df = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_server)
  .option("subscribe", "topic_0")
  .option("startingOffsets", "latest")
  .option("kafka.security.protocol","SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config",kafka_config) \
  .load()
  .limit(5)
)
schema = StructType([
    StructField("title", StringType(), True),
    StructField("link", StringType(), True),
    StructField("description", StringType(), True),
    StructField("published", StringType(), True),
    StructField("tag", StringType(), True),
])
# Extract JSON data and apply schema
json_df = (df
           .selectExpr("CAST(value AS STRING) as json")
           .select(from_json("json", schema).alias("data"))
           .select("data.*"))

def parse_medium_story_text(link):
    new_link = "https://freedium.cfd/"+link
    response = requests.get(new_link)
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')
    paragraphs = soup.find_all('div', {'class': 'main-content mt-8'})
    top_two_paragraphs = [p.get_text(strip=True) for p in paragraphs]

    story_text = ' '.join(top_two_paragraphs)

    return story_text

parse_medium_story_text_udf = udf(parse_medium_story_text, StringType())
link_column = json_df['link']
json_df_with_desc = json_df.withColumn("description", parse_medium_story_text_udf(link_column))
# print("Printing the extracted info with blog details ")
display(json_df_with_desc.limit(10))

# MongoDB configuration
collection_name = "feed_life_desc"

dataStreamWriter = json_df_with_desc.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "checkpoint")\
        .option("spark.mongodb.connection.uri", mongodb_uri) \
        .option("spark.mongodb.database", "rss_feed") \
        .option("spark.mongodb.collection", collection_name) \
        .outputMode("append")

# Start the MongoDB query
query_mongodb = dataStreamWriter.start()

# Await termination for both queries
query_mongodb.awaitTermination()

