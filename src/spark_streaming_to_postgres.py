import os
import logging
import traceback
from pyspark.sql import SparkSession, types as T

# Setup Logging
LOG_FILE = os.getenv("LOG_FILE", "/opt/spark/logs/streaming.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Environment Variables
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Schema Definition
event_schema = T.StructType([
    T.StructField("user_id", T.IntegerType(), True),
    T.StructField("session_id", T.StringType(), True),
    T.StructField("actions", T.StringType(), True),
    T.StructField("product_id", T.IntegerType(), True),
    T.StructField("price", T.DoubleType(), True),
    T.StructField("event_time", T.StringType(), True),
    T.StructField("user_agent", T.StringType(), True)
])

# Paths & DB Config
SPARK_DATA_PATH = "/opt/spark/data/events"
POSTGRES_URL = f"jdbc:postgresql://postgres:5432/{DB_NAME}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def process_batch(df, batch_id):
    try:
        logging.info(f"Processing batch {batch_id}")
        cleaned = df.na.drop(subset=["user_id", "session_id", "actions", "product_id", "event_time"])
        cleaned = cleaned.withColumn("price", cleaned["price"].cast("double"))
        cleaned = cleaned.withColumn("event_time", cleaned["event_time"].cast("timestamp"))

        cleaned.write.jdbc(
            url=POSTGRES_URL,
            table="events",
            mode="append",
            properties=POSTGRES_PROPERTIES
        )
        row_count = cleaned.count()
        logging.info(f"Batch {batch_id} written to Postgres: {row_count} rows.")
    except Exception as e:
        logging.error(f"Error processing batch {batch_id}")
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    try:
        logging.info("Starting Spark streaming job")
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
    except Exception as e:
        logging.critical("Fatal error in Spark streaming job")
        logging.critical(traceback.format_exc())


# from pyspark.sql import SparkSession, types as T
# import os

# POSTGRES_USER = os.getenv('POSTGRES_USER')
# POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
# DB_NAME = os.getenv('DB_NAME')


# # Define schema matching CSV and DB
# event_schema = T.StructType([
#     T.StructField("user_id", T.IntegerType(), True),
#     T.StructField("session_id", T.StringType(), True),
#     T.StructField("actions", T.StringType(), True),
#     T.StructField("product_id", T.IntegerType(), True),
#     T.StructField("price", T.DoubleType(), True),
#     T.StructField("event_time", T.StringType(), True),
#     T.StructField("user_agent", T.StringType(), True)
# ])

# SPARK_DATA_PATH = "/opt/spark/data/events"

# POSTGRES_URL = f"jdbc:postgresql://postgres:5432/{DB_NAME}"
# POSTGRES_PROPERTIES = {
#     "user": POSTGRES_USER,
#     "password": POSTGRES_PASSWORD,
#     "driver": "org.postgresql.Driver"
# }

# def process_batch(df, batch_id):
#     # Clean & convert
#     cleaned = df.na.drop(subset=["user_id", "session_id", "actions", "product_id", "event_time"])
#     cleaned = cleaned.withColumn("price", cleaned["price"].cast("double"))
#     cleaned = cleaned.withColumn("event_time", cleaned["event_time"].cast("timestamp"))
#     # Write to Postgres
#     cleaned.write.jdbc(
#         url=POSTGRES_URL,
#         table="events",
#         mode="append",
#         properties=POSTGRES_PROPERTIES
#     )
#     print(f"Batch {batch_id} written: {cleaned.count()} rows.")

# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("CSVtoPostgres").getOrCreate()

#     df = spark.readStream.format("csv") \
#         .option("header", True) \
#         .schema(event_schema) \
#         .load(SPARK_DATA_PATH)

#     query = df.writeStream \
#         .foreachBatch(process_batch) \
#         .option("checkpointLocation", "/tmp/checkpoints") \
#         .start()

#     query.awaitTermination()
