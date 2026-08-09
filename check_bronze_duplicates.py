from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("BronzeDuplicateCheck")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

bronze_path = "data/bronze/transactions"

print("\n======================================")
print("       BRONZE DATA DUPLICATE CHECK")
print("======================================")

bronze_df = spark.read.parquet(bronze_path)

total_count = bronze_df.count()

unique_count = (
    bronze_df
    .select("transaction_id")
    .distinct()
    .count()
)

duplicate_count = total_count - unique_count

print(f"\nTotal Bronze transactions: {total_count}")
print(f"Unique transaction IDs: {unique_count}")
print(f"Duplicate transaction records: {duplicate_count}")

duplicates = (
    bronze_df
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
    .orderBy("transaction_id")
)

duplicate_id_count = duplicates.count()

print(f"Duplicate transaction IDs: {duplicate_id_count}")

if duplicate_id_count > 0:
    print("\nDuplicate transaction IDs found:")
    duplicates.show(100, truncate=False)

    print("\nExample duplicate records:")
    duplicate_ids = duplicates.select("transaction_id")

    (
        bronze_df
        .join(duplicate_ids, "transaction_id", "inner")
        .orderBy("transaction_id")
        .show(10, truncate=False)
    )
else:
    print("\nNO DUPLICATES FOUND IN BRONZE.")

print("\n======================================")

spark.stop()