import pandas as pd
import random
import uuid
import os
from faker import Faker
from datetime import datetime

fake = Faker()

def generate_daily_transactions(num_records=100):
    data = []
    categories = ["Footwear", "Menswear", "Accessories", "Electronics"]
    
    for _ in range(num_records):
        record = {
            "order_id": str(uuid.uuid4()),
            "customer_id": fake.random_int(min=1000, max=9999),
            "category": random.choice(categories),
            "price": round(random.uniform(15.0, 150.0), 2),
            "order_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "review_text": fake.sentence(nb_words=8)
        }
        data.append(record)
        
    # INJECT BAD DATA 
    for _ in range(5): data.append(random.choice(data).copy())
    for i in range(5): data[i]["customer_id"] = None
    for i in range(5, 10): data[i]["price"] = -1 * data[i]["price"]
    for i in range(10, 15): data[i]["order_date"] = "invalid_date_format"

    random.shuffle(data)
    
    return pd.DataFrame(data)

def write_to_bronze_lake(df):
    # Create Hive partitions based on today's date
    now = datetime.now()
    partition_path = os.path.join(
        "data_lake", "bronze", 
        f"year={now.year}", 
        f"month={now.month:02d}", 
        f"day={now.day:02d}"
    )
    
    os.makedirs(partition_path, exist_ok=True)
    
    filename = f"orders_{now.strftime('%H%M%S')}.json"
    file_path = os.path.join(partition_path, filename)
    
    df.to_json(file_path, orient="records", lines=True)
    print(f"Successfully landed data in Bronze Lake: {file_path}")

if __name__ == "__main__":
    df = generate_daily_transactions(100)
    write_to_bronze_lake(df)