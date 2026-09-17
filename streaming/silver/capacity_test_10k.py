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
    .appName("FraudDetectionSilverCapacityTest10K")
    .config("spark.hadoop.io.native.lib.available", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("===================================")
print("Fraud Detection - Silver Layer 10K")
print("Spark Version:", spark.version)
print("===================================")
# -----------------------------------------
# 2. Define input schema
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
# 3. Read 10K capacity-test output
# -----------------------------------------
bronze_10k_df = (
    spark.read
    .schema(transaction_schema)
    .format("parquet")
    .load(
        "D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/data/capacity_test_10k"
    )
)
# -----------------------------------------
# 4. Clean and transform
# -----------------------------------------
silver_10k_df = (
    bronze_10k_df
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
    # Remove duplicate transactions
    .dropDuplicates(["transaction_id"])
    # Remove original string timestamp
    .drop("timestamp")
)
# -----------------------------------------
# 5. Write isolated 10K Silver output
# -----------------------------------------
output_path = (
    "D:/DataEngineeringProjects/"
    "RealTimeFraudDetectionPlatform/"
    "data/silver_capacity_test_10k"
)
silver_10k_df.write.mode("overwrite").parquet(output_path)
print("===================================")
print("10K Silver transformation completed")
print("Input:  data/capacity_test_10k")
print("Output: data/silver_capacity_test_10k")
print("===================================")
spark.stop()
