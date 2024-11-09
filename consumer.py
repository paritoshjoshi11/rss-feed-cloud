import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql.functions import from_json, udf ,col
import google.generativeai as genai
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()



kafka_config = os.environ.get('SPARK_KAFKA_CONFIG')
kafka_server = os.environ.get('SPARK_KAFKA_CLUSTER')
mongodb_uri= os.environ.get('NONGO_URI')
api_key= os.environ.get('GEMINI_API_KEY')


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
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", kafka_config)
      .load()
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

# Function to fetch story text from the given link
def parse_medium_story_text(link):
    try:
        new_link = "https://freedium.cfd/" + link
        response = requests.get(new_link)
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        paragraphs = soup.find_all('div', {'class': 'main-content mt-8'})
        top_two_paragraphs = [p.get_text(strip=True) for p in paragraphs]
        return ' '.join(top_two_paragraphs)
    except Exception as e:
        return f"Error fetching content: {str(e)}"

# Define the UDF
parse_medium_story_text_udf = udf(parse_medium_story_text, StringType())

# Apply UDF to add descriptions
json_df_with_desc = json_df.withColumn("description", parse_medium_story_text_udf(col("link")))


# Broadcast API Key to all workers
broadcast_api_key = spark.sparkContext.broadcast(api_key)

# Function to summarize the description using Gemini API
def summarise_desc(description):
    try:
        genai.configure(api_key=broadcast_api_key.value)  # Configure API key
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(f"Summarize the following text in 80 words: {description}")
        return response.text if response else "No summary generated."
    except Exception as e:
        return f"Error summarizing content: {str(e)}"

# Register UDF for summarization
summarise_desc_udf = udf(summarise_desc, StringType())

# Apply UDF to add summary
summarized_df = json_df_with_desc.withColumn("summary", summarise_desc_udf(col("description")))

# MongoDB Write Stream for summarized descriptions
dataStreamWriter_summary = summarized_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/temp/checkpoint") \
    .option("spark.mongodb.connection.uri", mongodb_uri) \
    .option("spark.mongodb.database", "rss_feed") \
    .option("spark.mongodb.collection", "feed_summary_desc") \
    .outputMode("append")

# Start the MongoDB write queries
query_summary = dataStreamWriter_summary.start()

# Await termination for both queries
query_summary.awaitTermination()
