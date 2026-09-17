from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
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
# 2. Read Silver as streaming source
# -----------------------------------------
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
silver_df = (
    spark.readStream
    .schema(silver_schema)
    .format("parquet")
    .load(
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/data/silver/transactions"
    )
)

# -----------------------------------------
# 3. Fraud detection rules
# -----------------------------------------

scored_df = (
    silver_df

    # High-value transaction
    .withColumn(
        "high_amount_flag",
        when(col("amount") >= 100000, 1).otherwise(0)
    )

    # Medium-high transaction
    .withColumn(
        "medium_amount_flag",
        when(
            (col("amount") >= 50000) &
            (col("amount") < 100000),
            1
        ).otherwise(0)
    )

    # Risk score
    .withColumn(
        "risk_score",
        col("high_amount_flag") * 70
        + col("medium_amount_flag") * 30
    )

    # Risk level
    .withColumn(
        "risk_level",
        when(col("risk_score") >= 70, "High")
        .when(col("risk_score") >= 30, "Medium")
        .otherwise("Low")
    )

    # Fraud flag
    .withColumn(
        "fraud_flag",
        when(col("risk_score") >= 70, 1).otherwise(0)
    )

    # Fraud reason
    .withColumn(
        "fraud_reason",
        when(
            col("high_amount_flag") == 1,
            lit("High transaction amount")
        )
        .when(
            col("medium_amount_flag") == 1,
            lit("Medium-high transaction amount")
        )
        .otherwise(
            lit("Normal transaction")
        )
    )

    # Remove intermediate rule columns
    .drop(
        "high_amount_flag",
        "medium_amount_flag"
    )
)

# -----------------------------------------
# 4. Write Gold layer
# -----------------------------------------

query = (
    scored_df
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

print("Gold fraud-scoring streaming started successfully!")
print("Reading from: data/silver/transactions")
print("Writing to:   data/gold/fraud_transactions")

query.awaitTermination()
