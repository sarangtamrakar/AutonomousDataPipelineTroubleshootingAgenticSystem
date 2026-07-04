import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

# set the aws credentials to os env
os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
os.environ['AWS_REGION'] = os.getenv('AWS_REGION')
print("AWS credentials loaded from .env file.")



# ==========================================
# 1. Configuration
# ==========================================
S3_BUCKET_PATH = "s3://sarang-de/raw" # Replace with your actual bucket
NUM_CUSTOMERS = 100
NUM_PRODUCTS = 50
NUM_SALES = 100

fake = Faker()

def generate_mock_data():
    print("Starting data generation...")

    # ==========================================
    # 2. Generate Products
    # ==========================================
    categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Sports', 'Books']
    products_data = []
    
    for i in range(1, NUM_PRODUCTS + 1):
        products_data.append({
            "product_id": f"P{str(i).zfill(4)}",
            "product_name": fake.word().capitalize() + " " + fake.word().capitalize(),
            "category": random.choice(categories),
            "unit_price": round(random.uniform(10.0, 500.0), 2)
        })
        
    df_products = pd.DataFrame(products_data)
    print(f"Generated {len(df_products)} products.")

    # ==========================================
    # 3. Generate Customers
    # ==========================================
    regions = ['North', 'South', 'East', 'West', 'Central']
    customers_data = []
    
    for i in range(1, NUM_CUSTOMERS + 1):
        customers_data.append({
            "customer_id": f"C{str(i).zfill(4)}",
            "customer_name": fake.name(),
            "region": random.choice(regions)
        })
        
    df_customers = pd.DataFrame(customers_data)
    print(f"Generated {len(df_customers)} customers.")

    # ==========================================
    # 4. Generate Sales (Fact Table)
    # ==========================================
    sales_data = []
    start_date = datetime.now() - timedelta(days=365) # Sales from the last year
    
    # Extract IDs for foreign key mapping
    customer_ids = df_customers['customer_id'].tolist()
    product_ids = df_products['product_id'].tolist()
    
    # Pre-generate a small pool of dates so multiple sales share the same date (partition-friendly)
    date_pool = [
        fake.date_between(start_date=start_date, end_date='today').strftime('%Y-%m-%d')
        for _ in range(30)  # ~30 distinct dates across 365 days → good partition distribution
    ]

    for i in range(1, NUM_SALES + 1):
        sales_data.append({
            "sale_id": f"S{str(i).zfill(6)}",
            "customer_id": random.choice(customer_ids),
            "product_id": random.choice(product_ids),
            "quantity_sold": random.randint(1, 10),
            "sale_date": random.choice(date_pool),
        })
        
    df_sales = pd.DataFrame(sales_data)
    print(f"Generated {len(df_sales)} sales records.")

    # ==========================================
    # 5. Upload directly to AWS S3
    # ==========================================
    print(f"\nUploading to {S3_BUCKET_PATH}...")
    
    try:
        # index=False ensures the pandas index column isn't written to the CSV
        df_customers.to_csv(f"{S3_BUCKET_PATH}/customers/customers.csv", index=False)
        print(f"Successfully uploaded: {S3_BUCKET_PATH}/customers/customers.csv")
        
        df_products.to_csv(f"{S3_BUCKET_PATH}/products/products.csv", index=False)
        print(f"Successfully uploaded: {S3_BUCKET_PATH}/products/products.csv")
        
        df_sales.to_csv(f"{S3_BUCKET_PATH}/sales/sales.csv", index=False)
        print(f"Successfully uploaded: {S3_BUCKET_PATH}/sales/sales.csv")
        
        print("\nAll mock data uploaded to S3 raw layer!")
        
    except Exception as e:
        print(f"\nError uploading to S3: {e}")
        print("Tip: Check if your AWS credentials are set and if the IAM role has s3:PutObject permissions.")

if __name__ == "__main__":
    generate_mock_data()