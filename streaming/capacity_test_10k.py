from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# -----------------------------------------
# 1. Spark session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("FraudDetectionCapacityTest10K")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"
    )
    .config(
        "spark.hadoop.io.native.lib.available",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("==========================================")
print("Spark 10K Capacity Test")
print("==========================================")
print("Spark Version:", spark.version)
print("==========================================")


# -----------------------------------------
# 2. Kafka source
# -----------------------------------------

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions_capacity_10k")
    .option("startingOffsets", "earliest")
    .load()
)


# -----------------------------------------
# 3. Transaction schema
# -----------------------------------------

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", StringType(), True)
])


# -----------------------------------------
# 4. Parse JSON
# -----------------------------------------

transactions = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(
            col("json_value"),
            transaction_schema
        ).alias("data")
    )
    .select("data.*")
)


# -----------------------------------------
# 5. Write to isolated test output
# -----------------------------------------

query = (
    transactions
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/data/capacity_test_10k"
    )
    .option(
        "checkpointLocation",
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/checkpoints/capacity_test_10k"
    )
    .start()
)

print("==========================================")
print("Spark capacity test started")
print("Reading : transactions_capacity_10k")
print("Writing : data/capacity_test_10k")
print("==========================================")

query.awaitTermination()