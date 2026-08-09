from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# =========================================================
# 1. Create Spark session
# =========================================================

spark = (
    SparkSession.builder
    .appName("CheckGoldDuplicates")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 2. Gold data location
# =========================================================

gold_path = (
    "D:/DataEngineeringProjects/"
    "RealTimeFraudDetectionPlatform/"
    "data/gold/fraud_transactions"
)


# =========================================================
# 3. Read Gold data
# =========================================================

gold_df = spark.read.parquet(gold_path)

print("======================================")
print("GOLD DATA DUPLICATE CHECK")
print("======================================")

print(
    "Total Gold transactions:",
    gold_df.count()
)


# =========================================================
# 4. Find duplicate transaction IDs
# =========================================================

duplicate_transactions = (
    gold_df
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
)


# =========================================================
# 5. Count duplicate transaction IDs
# =========================================================

duplicate_count = duplicate_transactions.count()

print(
    "Duplicate transaction IDs:",
    duplicate_count
)


# =========================================================
# 6. Show duplicate transaction IDs
# =========================================================

if duplicate_count > 0:

    print("\nDuplicate transaction IDs found:")

    duplicate_transactions.show(
        50,
        truncate=False
    )

else:

    print("\nNo duplicate transaction IDs found in Gold.")


# =========================================================
# 7. Specifically check TXN00000064
# =========================================================

print("\n======================================")
print("CHECKING TXN00000064")
print("======================================")

gold_df.filter(
    col("transaction_id") == "TXN00000064"
).show(
    truncate=False
)


# =========================================================
# 8. Stop Spark
# =========================================================

spark.stop()
