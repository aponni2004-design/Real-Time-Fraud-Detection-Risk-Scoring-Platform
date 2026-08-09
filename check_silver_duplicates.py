from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("CheckSilverDuplicates")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# SILVER PATH
# =========================================================

silver_path = (
    "D:/DataEngineeringProjects/"
    "RealTimeFraudDetectionPlatform/"
    "data/silver/transactions"
)

# =========================================================
# READ SILVER
# =========================================================

silver_df = spark.read.parquet(silver_path)

# =========================================================
# BASIC COUNTS
# =========================================================

total_count = silver_df.count()

unique_count = (
    silver_df
    .select("transaction_id")
    .distinct()
    .count()
)

print("======================================")
print("SILVER DATA DUPLICATE CHECK")
print("======================================")

print("Total Silver transactions:", total_count)
print("Unique transaction IDs:", unique_count)
print(
    "Duplicate transaction records:",
    total_count - unique_count
)

# =========================================================
# FIND DUPLICATE TRANSACTION IDs
# =========================================================

duplicate_ids = (
    silver_df
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
    .orderBy(col("count").desc())
)

duplicate_id_count = duplicate_ids.count()

print(
    "Duplicate transaction IDs:",
    duplicate_id_count
)

# =========================================================
# DISPLAY DUPLICATES
# =========================================================

if duplicate_id_count > 0:

    print()
    print("Duplicate transaction IDs found:")
    
    duplicate_ids.show(
        100,
        truncate=False
    )

else:

    print()
    print("NO duplicate transaction IDs found in Silver.")

# =========================================================
# CHECK TXN00000064
# =========================================================

print()
print("======================================")
print("CHECKING TXN00000064")
print("======================================")

silver_df.filter(
    col("transaction_id") == "TXN00000064"
).show(
    truncate=False
)

print("======================================")

spark.stop()