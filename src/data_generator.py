from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Start the Spark session
    spark = SparkSession.builder \
        .appName("TestSparkSession") \
        .getOrCreate()
    print("Spark version:", spark.version)

    # Make a tiny DataFrame
    df = spark.createDataFrame([("Alice", 1), ("Bob", 2)], ["name", "id"])
    df.show()

    # Stop the session
    spark.stop()
