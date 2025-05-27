This activity is a **Mini ETL (Extract, Transform, Load) Task** that reads raw sales data from an **AWS S3** bucket, processes it using **Pandas**, and then uploads the transformed data back to the same S3 bucket.

### Tools Used

- **Python** – for scripting the ETL logic  
- **Pandas** – for data manipulation and transformation  
- **boto3** – to connect and interact with AWS S3  
- **python-dotenv** – to manage AWS credentials securely from a `.env` file  
- **AWS S3** – as the cloud storage source and destination
