from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_timestamp
from pyspark.sql.types import StringType
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import os
from datetime import datetime

# Download the lightweight sentiment dictionary (runs once and caches locally)
nltk.download('vader_lexicon', quiet=True)

def process_silver_layer():
    # 1. Initialize Spark (Local Mode)
    spark = SparkSession.builder \
        .appName("BronzeToSilverPipeline") \
        .getOrCreate()

    print("Reading from Bronze Layer...")
    # Read all JSON files in the bronze data lake
    bronze_path = "data_lake/bronze/*/*/*/*.json"
    
    try:
        df = spark.read.json(bronze_path)
    except Exception as e:
        print(f"Error reading Bronze data. Make sure data_generator.py ran. Error: {e}")
        return

    print(f"Initial Row Count: {df.count()}")

    # 2. DATA QUALITY TRANSFORMATIONS (The "Cleaning" Phase)
    
    # A. Drop Duplicates based on the unique order_id
    df_clean = df.dropDuplicates(["order_id"])
    print(f"Row count after dropping duplicates: {df_clean.count()}")

    # B. Handle Nulls (Drop rows where customer_id is missing)
    df_clean = df_clean.dropna(subset=["customer_id"])
    print(f"Row count after dropping null customers: {df_clean.count()}")

    # C. Filter Data Quality Violations (Prices cannot be negative)
    df_clean = df_clean.filter(col("price") > 0)
    print(f"Row count after filtering negative prices: {df_clean.count()}")

    # D. Standardize Dates (Convert string to Timestamp, drop corrupted formats)
    df_clean = df_clean.withColumn("order_date_ts", to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss"))
    df_clean = df_clean.dropna(subset=["order_date_ts"])
    df_clean = df_clean.drop("order_date").withColumnRenamed("order_date_ts", "order_date")
    print(f"Row count after date standardization: {df_clean.count()}")


    # 3. DATA ENRICHMENT (Sentiment Analysis)
    print("Applying Sentiment Enrichment...")
    
    def get_sentiment(text):
        if not text:
            return "Neutral"
        sia = SentimentIntensityAnalyzer()
        score = sia.polarity_scores(text)['compound']
        if score >= 0.05:
            return "Positive"
        elif score <= -0.05:
            return "Negative"
        else:
            return "Neutral"

    # Register the Python function as a PySpark User Defined Function (UDF)
    sentiment_udf = udf(get_sentiment, StringType())
    
    df_enriched = df_clean.withColumn("review_sentiment", sentiment_udf(col("review_text")))


    # 4. WRITE TO SILVER LAYER
    now = datetime.now()
    silver_path = os.path.join(
        "data_lake", "silver", 
        f"year={now.year}", 
        f"month={now.month:02d}", 
        f"day={now.day:02d}"
    )
    
    print(f"Writing clean, enriched data to Silver Lake in Parquet format at: {silver_path}")
    
    # Write as Parquet (Industry standard for Data Lakes)
    df_enriched.write.mode("overwrite").parquet(silver_path)
    
    print("Silver layer processing complete! Pipeline successful.")

if __name__ == "__main__":
    process_silver_layer()
