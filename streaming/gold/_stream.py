from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit
)

# -----------------------------------------
# 1. Create Spark session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("FraudDetectionGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("===================================")
print("Fraud Detection - Gold Layer")
print("Spark Version:", spark.version)
print("===================================")


# -----------------------------------------
# 2. Define Silver schema
# -----------------------------------------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

silver_schema = StructType([
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
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("amount_category", StringType(), True)
])


# -----------------------------------------
# 3. Read Silver as streaming source
# -----------------------------------------

silver_df = (
    spark.readStream
    .schema(silver_schema)
    .format("parquet")
    .load(
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/data/silver/transactions"
    )
)


# -----------------------------------------
# 4. Rule-based fraud scoring
# -----------------------------------------

gold_df = (
    silver_df

    # Start every transaction with score 0
    .withColumn("risk_score", lit(0))

    # Rule 1: High-value transaction
    .withColumn(
        "risk_score",
        col("risk_score") +
        when(col("amount") > 40000, 30).otherwise(0)
    )

    # Rule 2: Very high-value transaction
    .withColumn(
        "risk_score",
        col("risk_score") +
        when(col("amount") > 75000, 30).otherwise(0)
    )

    # Rule 3: Missing device information
    .withColumn(
        "risk_score",
        col("risk_score") +
        when(col("device_id").isNull(), 20).otherwise(0)
    )

    # Rule 4: Missing merchant information
    .withColumn(
        "risk_score",
        col("risk_score") +
        when(col("merchant_id").isNull(), 10).otherwise(0)
    )

    # -----------------------------------------
    # Risk level
    # -----------------------------------------

    .withColumn(
        "risk_level",
        when(col("risk_score") >= 60, "HIGH")
        .when(col("risk_score") >= 30, "MEDIUM")
        .otherwise("LOW")
    )

    # -----------------------------------------
    # Fraud flag
    # -----------------------------------------

    .withColumn(
        "fraud_flag",
        when(col("risk_score") >= 60, 1)
        .otherwise(0)
    )
)


# -----------------------------------------
# 5. Write Gold data
# -----------------------------------------

query = (
    gold_df
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/data/gold/fraud_transactions"
    )
    .option(
        "checkpointLocation",
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/checkpoints/gold/fraud_transactions"
    )
    .start()
)

print("===================================")
print("Gold streaming started successfully!")
print("Reading from: data/silver/transactions")
print("Writing to:   data/gold/fraud_transactions")
print("===================================")

query.awaitTermination()