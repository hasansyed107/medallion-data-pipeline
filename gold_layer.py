from pyspark.sql import SparkSession
import os
from datetime import datetime

def process_gold_layer():
    spark = SparkSession.builder \
        .appName("SilverToGoldPipeline") \
        .getOrCreate()

    print("Reading enriched data from Silver Layer...")
    silver_path = "data_lake/silver/*/*/*/*.parquet"
    
    try:
        df_silver = spark.read.parquet(silver_path)
    except Exception as e:
        print(f"Error reading Silver data. Make sure silver_layer.py ran. Error: {e}")
        return
        
    # Register the dataframe as a temporary SQL view
    df_silver.createOrReplaceTempView("silver_orders")
    
    print("Executing Gold Layer SQL Aggregations...")
    
    # 1. Gold Metric: Daily Revenue and Order Count by Category
    revenue_sql = """
        SELECT 
            category,
            COUNT(order_id) as total_orders,
            ROUND(SUM(price), 2) as total_revenue,
            ROUND(AVG(price), 2) as average_order_value
        FROM silver_orders
        GROUP BY category
        ORDER BY total_revenue DESC
    """
    df_gold_revenue = spark.sql(revenue_sql)
    
    # 2. Gold Metric: Sentiment Breakdown by Category
    sentiment_sql = """
        SELECT 
            category,
            review_sentiment,
            COUNT(order_id) as review_count
        FROM silver_orders
        GROUP BY category, review_sentiment
        ORDER BY category, review_sentiment
    """
    df_gold_sentiment = spark.sql(sentiment_sql)
    
    # Show the results in the terminal
    print("\n--- GOLD LAYER: REVENUE BY CATEGORY ---")
    df_gold_revenue.show()
    
    print("\n--- GOLD LAYER: SENTIMENT BY CATEGORY ---")
    df_gold_sentiment.show()

    # Save the Gold tables to the data lake
    now = datetime.now()
    gold_base_path = os.path.join(
        "data_lake", "gold", 
        f"year={now.year}", 
        f"month={now.month:02d}", 
        f"day={now.day:02d}"
    )
    
    df_gold_revenue.write.mode("overwrite").parquet(os.path.join(gold_base_path, "revenue_summary"))
    df_gold_sentiment.write.mode("overwrite").parquet(os.path.join(gold_base_path, "sentiment_summary"))
    
    print(f"Gold layer processing complete! Aggregated tables saved to: {gold_base_path}")

if __name__ == "__main__":
    process_gold_layer()
