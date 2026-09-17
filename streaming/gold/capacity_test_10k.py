from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
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
    .appName("FraudDetectionGoldCapacityTest10K")
    .config("spark.hadoop.io.native.lib.available", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("===================================")
print("Fraud Detection - Gold Layer 10K")
print("Spark Version:", spark.version)
print("===================================")
# -----------------------------------------
# 2. Read isolated 10K Silver data
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
silver_10k_df = (
    spark.read
    .schema(silver_schema)
    .format("parquet")
    .load(
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/data/silver_capacity_test_10k"
    )
)
# -----------------------------------------
# 3. Apply current fraud detection rules
# -----------------------------------------
scored_10k_df = (
    silver_10k_df
    .withColumn(
        "high_amount_flag",
        when(col("amount") >= 100000, 1).otherwise(0)
    )
    .withColumn(
        "medium_amount_flag",
        when(
            (col("amount") >= 50000) &
            (col("amount") < 100000),
            1
        ).otherwise(0)
    )
    .withColumn(
        "risk_score",
        col("high_amount_flag") * 70
        + col("medium_amount_flag") * 30
    )
    .withColumn(
        "risk_level",
        when(col("risk_score") >= 70, "High")
        .when(col("risk_score") >= 30, "Medium")
        .otherwise("Low")
    )
    .withColumn(
        "fraud_flag",
        when(col("risk_score") >= 70, 1).otherwise(0)
    )
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
    .drop(
        "high_amount_flag",
        "medium_amount_flag"
    )
)
# -----------------------------------------
# 4. Write isolated 10K Gold output
# -----------------------------------------
scored_10k_df.write.mode("overwrite").parquet(
    "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/data/gold_capacity_test_10k"
)
print("===================================")
print("10K Gold transformation completed")
print("Input:  data/silver_capacity_test_10k")
print("Output: data/gold_capacity_test_10k")
print("===================================")
spark.stop()
