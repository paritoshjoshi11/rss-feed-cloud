import requests
from bs4 import BeautifulSoup
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql.functions import from_json, udf
import google.generativeai as genai


df = (spark
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "pkc-12576z.us-west2.gcp.confluent.cloud:9092")
  .option("subscribe", "topic_0")
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .option("kafka.security.protocol","SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="TJZX5TLU7WKVH7T2" password="lVvPo03Ubd4LxniHzDPhGYxkty0GpITy5jOmZ2/ebARq7kP+au6XBG5QWhX1RLg9";""") \
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

# Display the structured data in the console for testing
#display(json_df)

def summarise_desc(description_text):
    genai.configure(api_key='AIzaSyA_YedBCTKumJym9LJTJb6rYoWwpdamFw0')
    model = genai.GenerativeModel("gemini-1.5-flash")
    response = model.generate_content(f"Give summary on {description_text} in 100 words")
    return response

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

# summarise_description_udf = udf(summarise_desc, StringType())
# desc_column = json_df_with_desc['description']
# json_df_with_desc_sum = json_df_with_desc.withColumn("summary", summarise_description_udf(desc_column))

# Print the received events to the console
display(json_df_with_desc.limit(10))
# display(json_df_with_desc_sum.limit(10))
