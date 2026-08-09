from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    trim,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# -----------------------------------------
# 1. Create Spark session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("FraudDetectionSilver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("===================================")
print("Fraud Detection - Silver Layer")
print("Spark Version:", spark.version)
print("===================================")

# -----------------------------------------
# 2. Define Bronze schema
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
# 3. Read Bronze as streaming source
# -----------------------------------------

bronze_df = (
    spark.readStream
    .schema(transaction_schema)
    .format("parquet")
    .load(
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/data/bronze/transactions"
    )
)

# -----------------------------------------
# 4. Clean and transform
# -----------------------------------------

silver_df = (
    bronze_df

    .withColumn("transaction_id", trim(col("transaction_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("currency", trim(col("currency")))
    .withColumn("merchant_id", trim(col("merchant_id")))
    .withColumn("merchant_category", trim(col("merchant_category")))
    .withColumn("payment_method", trim(col("payment_method")))
    .withColumn("device_id", trim(col("device_id")))
    .withColumn("city", trim(col("city")))
    .withColumn("country", trim(col("country")))

    # Convert timestamp string to timestamp
    .withColumn(
        "transaction_timestamp",
        to_timestamp(col("timestamp"))
    )

    # Keep transactions with required fields
    .filter(
        col("transaction_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("amount").isNotNull()
        & (col("amount") > 0)
    )

    # Categorize transaction amount
    .withColumn(
        "amount_category",
        when(col("amount") < 1000, "Low")
        .when(col("amount") < 10000, "Medium")
        .otherwise("High")
    )

    # Remove original string timestamp
    .drop("timestamp")
)

# -----------------------------------------
# 5. Write Silver
# -----------------------------------------

query = (
    silver_df
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/data/silver/transactions"
    )
    .option(
        "checkpointLocation",
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/checkpoints/silver/transactions"
    )
    .start()
)

print("Silver streaming started successfully!")
print("Reading from: data/bronze/transactions")
print("Writing to:   data/silver/transactions")

query.awaitTermination()