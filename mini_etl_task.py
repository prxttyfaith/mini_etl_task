import os
import pandas as pd
import boto3
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name = os.getenv('AWS_BUCKET_NAME')
region = os.getenv('AWS_REGION')

input_key = 'raw_sales_data.csv'
output_key = 'processed_electronics_sales.csv'

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region
)

# Extract data from S3
obj = s3.get_object(Bucket=bucket_name, Key=input_key)
raw_data = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
raw_data.head()

# Transform
# Filter only Electronics category
filtered_data = raw_data[raw_data['category'] == 'Electronics'].copy()
# Add total_revenue column
filtered_data['total_revenue'] = filtered_data['quantity_sold'] * filtered_data['price_per_unit']
# Format sale_date
filtered_data.loc[:, 'sale_date'] = pd.to_datetime(filtered_data['sale_date']).dt.strftime('%Y-%m-%d')
# Reorder columns
filtered_data = filtered_data[['sale_id', 'product_name', 'category', 'total_revenue', 'sale_date', 'region']]
filtered_data.head()

# Load: save the transformed data to a new CSV file
csv_buffer = StringIO()
filtered_data.to_csv(csv_buffer, index=False)

s3.put_object(Bucket=bucket_name, Key=output_key, Body=csv_buffer.getvalue())
print(f"Processed data saved to {output_key} in S3. ")