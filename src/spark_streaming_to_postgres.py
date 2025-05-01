from pyspark.sql import SparkSession, types as T
import os
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('DB_NAME')


# Define schema matching CSV and DB
event_schema = T.StructType([
    T.StructField("user_id", T.IntegerType(), True),
    T.StructField("session_id", T.StringType(), True),
    T.StructField("actions", T.StringType(), True),
    T.StructField("product_id", T.IntegerType(), True),
    T.StructField("price", T.DoubleType(), True),
    T.StructField("event_time", T.StringType(), True),
    T.StructField("user_agent", T.StringType(), True)
])

SPARK_DATA_PATH = "/opt/spark/data/events"

POSTGRES_URL = f"jdbc:postgresql://postgres:5432/{DB_NAME}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def process_batch(df, batch_id):
    # Clean & convert
    cleaned = df.na.drop(subset=["user_id", "session_id", "actions", "product_id", "event_time"])
    cleaned = cleaned.withColumn("price", cleaned["price"].cast("double"))
    cleaned = cleaned.withColumn("event_time", cleaned["event_time"].cast("timestamp"))
    # Write to Postgres
    cleaned.write.jdbc(
        url=POSTGRES_URL,
        table="events",
        mode="append",
        properties=POSTGRES_PROPERTIES
    )
    print(f"Batch {batch_id} written: {cleaned.count()} rows.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CSVtoPostgres").getOrCreate()

    df = spark.readStream.format("csv") \
        .option("header", True) \
        .schema(event_schema) \
        .load(SPARK_DATA_PATH)

    query = df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .start()

    query.awaitTermination()
