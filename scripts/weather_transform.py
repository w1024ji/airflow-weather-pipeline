import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

BUCKET_NAME = "data-pipeline-weather-project-2025-12"
RAW_S3_PATH = f"s3a://{BUCKET_NAME}/raw/weather_data_20251202.csv"
OUTPUT_S3_PATH = f"s3a://{BUCKET_NAME}/staging/processed/"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WeatherSparkTransform").getOrCreate()
    
    try:
        print(f"Reading CSV from: {RAW_S3_PATH}")
        df = spark.read.csv(
            RAW_S3_PATH,
            header=True,
            inferSchema=True
        )
        print(f"Successfully read {df.count()} rows")
        df.printSchema()
    except Exception as e:
        print(f"Error reading CSV from S3: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)
    
    # Process the data
    processed_df = df.withColumn("city", col("city").cast("string")) \
                     .withColumn("temperature", col("temperature").cast("double")) \
                     .withColumn("processing_time", current_timestamp()) \
                     .filter(col("temperature").isNotNull())  # FIXED: was isNotNulll()
    
    print(f"Processed {processed_df.count()} rows after filtering")
    
    try:
        print(f"Writing Parquet to: {OUTPUT_S3_PATH}")
        processed_df.write.mode("overwrite").parquet(OUTPUT_S3_PATH)
        print(f"✓ Successfully wrote Parquet data to {OUTPUT_S3_PATH}")
    except Exception as e:
        print(f"Error writing Parquet to S3: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)
    
    spark.stop()
    print("Spark job completed successfully")

