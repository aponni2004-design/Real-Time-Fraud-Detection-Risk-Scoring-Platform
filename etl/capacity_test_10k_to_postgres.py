from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    dayofmonth,
    quarter
)

# =========================================================
# 1. CREATE SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("GoldToPostgres")
    .config(
        "spark.jars",
        "file:///D:/DataEngineeringProjects/RealTimeFraudDetectionPlatform/postgresql-42.7.7.jar"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("======================================")
print("Gold → PostgreSQL ETL")
print("Spark Version:", spark.version)
print("======================================")


# =========================================================
# 2. READ GOLD DATA
# =========================================================

gold_path = (
    "D:/DataEngineeringProjects/"
    "RealTimeFraudDetectionPlatform/"
    "data/gold_capacity_test_10k"
)

gold_df = spark.read.parquet(gold_path)

gold_count = gold_df.count()

print("Gold transactions:", gold_count)

if gold_count == 0:
    print("No Gold transactions found.")
    spark.stop()
    exit()


# =========================================================
# 3. POSTGRESQL CONNECTION
# =========================================================

jdbc_url = "jdbc:postgresql://localhost:5432/fraud_detection_capacity_10k"

properties = {
    "user": "postgres",
    "password": "Ponni123",
    "driver": "org.postgresql.Driver"
}


# =========================================================
# 4. CUSTOMER DIMENSION
# =========================================================

print("\n======================================")
print("CUSTOMER DIMENSION")
print("======================================")

customer_df = (
    gold_df
    .select("customer_id")
    .filter(col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
)

existing_customer_df = (
    spark.read.jdbc(
        url=jdbc_url,
        table="dim_customer",
        properties=properties
    )
    .select("customer_id")
    .filter(col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
)

new_customer_df = (
    customer_df
    .join(
        existing_customer_df,
        on="customer_id",
        how="left_anti"
    )
)

new_customer_count = new_customer_df.count()

print("New customers to load:", new_customer_count)

if new_customer_count > 0:

    new_customer_df.write.jdbc(
        url=jdbc_url,
        table="dim_customer",
        mode="append",
        properties=properties
    )

    print("Customer dimension loaded.")

else:

    print("No new customers to load.")


# =========================================================
# 5. MERCHANT DIMENSION
# =========================================================

print("\n======================================")
print("MERCHANT DIMENSION")
print("======================================")

merchant_df = (
    gold_df
    .select(
        "merchant_id",
        "merchant_category"
    )
    .filter(col("merchant_id").isNotNull())
    .dropDuplicates(["merchant_id"])
)

existing_merchant_df = (
    spark.read.jdbc(
        url=jdbc_url,
        table="dim_merchant",
        properties=properties
    )
    .select("merchant_id")
    .filter(col("merchant_id").isNotNull())
    .dropDuplicates(["merchant_id"])
)

new_merchant_df = (
    merchant_df
    .join(
        existing_merchant_df,
        on="merchant_id",
        how="left_anti"
    )
)

new_merchant_count = new_merchant_df.count()

print("New merchants to load:", new_merchant_count)

if new_merchant_count > 0:

    new_merchant_df.write.jdbc(
        url=jdbc_url,
        table="dim_merchant",
        mode="append",
        properties=properties
    )

    print("Merchant dimension loaded.")

else:

    print("No new merchants to load.")


# =========================================================
# 6. DEVICE DIMENSION
# =========================================================

print("\n======================================")
print("DEVICE DIMENSION")
print("======================================")

device_df = (
    gold_df
    .select("device_id")
    .filter(col("device_id").isNotNull())
    .dropDuplicates(["device_id"])
)

existing_device_df = (
    spark.read.jdbc(
        url=jdbc_url,
        table="dim_device",
        properties=properties
    )
    .select("device_id")
    .filter(col("device_id").isNotNull())
    .dropDuplicates(["device_id"])
)

# IMPORTANT:
# Only devices that do NOT already exist in PostgreSQL

new_device_df = (
    device_df
    .join(
        existing_device_df,
        on="device_id",
        how="left_anti"
    )
)

new_device_count = new_device_df.count()

null_device_count = (
    new_device_df
    .filter(col("device_id").isNull())
    .count()
)

print("New devices to load:", new_device_count)
print("NULL device IDs after cleaning:", null_device_count)

if new_device_count > 0:

    new_device_df.write.jdbc(
        url=jdbc_url,
        table="dim_device",
        mode="append",
        properties=properties
    )

    print("Device dimension loaded.")

else:

    print("No new devices to load.")


# =========================================================
# 7. DATE DIMENSION
# =========================================================

print("\n======================================")
print("DATE DIMENSION")
print("======================================")

date_df = (
    gold_df
    .select(
        to_date(
            col("transaction_timestamp")
        ).alias("full_date")
    )
    .filter(col("full_date").isNotNull())
    .dropDuplicates(["full_date"])
    .withColumn(
        "date_key",
        year("full_date") * 10000
        + month("full_date") * 100
        + dayofmonth("full_date")
    )
    .withColumn(
        "year",
        year("full_date")
    )
    .withColumn(
        "month",
        month("full_date")
    )
    .withColumn(
        "day",
        dayofmonth("full_date")
    )
    .withColumn(
        "quarter",
        quarter("full_date")
    )
    .select(
        "date_key",
        "full_date",
        "year",
        "month",
        "day",
        "quarter"
    )
)

existing_date_df = (
    spark.read.jdbc(
        url=jdbc_url,
        table="dim_date",
        properties=properties
    )
    .select("full_date")
    .filter(col("full_date").isNotNull())
    .dropDuplicates(["full_date"])
)

new_date_df = (
    date_df
    .join(
        existing_date_df,
        on="full_date",
        how="left_anti"
    )
)

new_date_count = new_date_df.count()

print("New dates to load:", new_date_count)

if new_date_count > 0:

    new_date_df.write.jdbc(
        url=jdbc_url,
        table="dim_date",
        mode="append",
        properties=properties
    )

    print("Date dimension loaded.")

else:

    print("No new dates to load.")


# =========================================================
# 8. READ ALL DIMENSION KEYS
# =========================================================

print("\n======================================")
print("READING DIMENSION KEYS")
print("======================================")

customer_keys = (
    spark.read.jdbc(
        url=jdbc_url,
        table="dim_customer",
        properties=properties
    )
    .select(
        "customer_id",
        "customer_key"
    )
)

merchant_keys = (
    spark.read.jdbc(
        url=jdbc_url,
        table="dim_merchant",
        properties=properties
    )
    .select(
        "merchant_id",
        "merchant_key"
    )
)

device_keys = (
    spark.read.jdbc(
        url=jdbc_url,
        table="dim_device",
        properties=properties
    )
    .select(
        "device_id",
        "device_key"
    )
)

date_keys = (
    spark.read.jdbc(
        url=jdbc_url,
        table="dim_date",
        properties=properties
    )
    .select(
        "full_date",
        "date_key"
    )
)

print("Dimension keys loaded successfully.")


# =========================================================
# 9. READ EXISTING FACT TRANSACTIONS
# =========================================================

print("\n======================================")
print("CHECKING EXISTING FACT TRANSACTIONS")
print("======================================")

existing_fact_df = (
    spark.read.jdbc(
        url=jdbc_url,
        table="fact_transactions",
        properties=properties
    )
    .select("transaction_id")
    .filter(col("transaction_id").isNotNull())
    .dropDuplicates(["transaction_id"])
)

print(
    "Existing fact transactions:",
    existing_fact_df.count()
)

# =========================================================
# 10. REMOVE DUPLICATES AND KEEP ONLY NEW TRANSACTIONS
# =========================================================

print("\n======================================")
print("PREPARING NEW TRANSACTIONS")
print("======================================")

# Check duplicate transaction IDs inside Gold

gold_duplicate_df = (
    gold_df
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
)

gold_duplicate_count = gold_duplicate_df.count()

print(
    "Duplicate transaction IDs inside Gold:",
    gold_duplicate_count
)

if gold_duplicate_count > 0:

    print("WARNING: Duplicate transaction IDs found in Gold.")

    gold_duplicate_df.show(
        20,
        truncate=False
    )

# Remove duplicate transaction IDs inside Gold
# Keep one record for each transaction_id

deduplicated_gold_df = (
    gold_df
    .filter(col("transaction_id").isNotNull())
    .dropDuplicates(["transaction_id"])
)

print(
    "Gold transactions after deduplication:",
    deduplicated_gold_df.count()
)

# Compare against PostgreSQL fact table

new_gold_df = (
    deduplicated_gold_df
    .join(
        existing_fact_df,
        on="transaction_id",
        how="left_anti"
    )
)

new_gold_count = new_gold_df.count()

print(
    "New transactions to load:",
    new_gold_count
)

if new_gold_count == 0:

    print("No new transactions to load.")

    print("\n======================================")
    print("ETL COMPLETED - NOTHING NEW TO LOAD")
    print("======================================")

    spark.stop()
    exit()


# =========================================================
# 11. BUILD FACT TABLE
# =========================================================

print("\n======================================")
print("BUILDING FACT TABLE")
print("======================================")

fact_df = (
    new_gold_df

    # -------------------------------------
    # Customer surrogate key
    # -------------------------------------
    .join(
        customer_keys,
        on="customer_id",
        how="left"
    )

    # -------------------------------------
    # Merchant surrogate key
    # -------------------------------------
    .join(
        merchant_keys,
        on="merchant_id",
        how="left"
    )

    # -------------------------------------
    # Device surrogate key
    # -------------------------------------
    .join(
        device_keys,
        on="device_id",
        how="left"
    )

    # -------------------------------------
    # Create transaction date
    # -------------------------------------
    .withColumn(
        "full_date",
        to_date(col("transaction_timestamp"))
    )

    # -------------------------------------
    # Date surrogate key
    # -------------------------------------
    .join(
        date_keys,
        on="full_date",
        how="left"
    )

    # -------------------------------------
    # Select final fact columns
    # -------------------------------------
    .select(
        "transaction_id",
        col("customer_key"),
        col("merchant_key"),
        col("device_key"),
        col("date_key"),
        "amount",
        "currency",
        "payment_method",
        "city",
        "country",
        "transaction_timestamp",
        "amount_category",
        "risk_score",
        "risk_level",
        "fraud_flag"
    )
)


# =========================================================
# FACT DATA QUALITY CHECKS
# =========================================================

print("\n======================================")
print("FACT DATA QUALITY CHECKS")
print("======================================")

fact_count = fact_df.count()

print("Fact records prepared:", fact_count)

null_customer_keys = (
    fact_df
    .filter(col("customer_key").isNull())
    .count()
)

null_merchant_keys = (
    fact_df
    .filter(col("merchant_key").isNull())
    .count()
)

null_device_keys = (
    fact_df
    .filter(col("device_key").isNull())
    .count()
)

null_date_keys = (
    fact_df
    .filter(col("date_key").isNull())
    .count()
)

null_transaction_ids = (
    fact_df
    .filter(col("transaction_id").isNull())
    .count()
)

print("NULL customer keys:", null_customer_keys)
print("NULL merchant keys:", null_merchant_keys)
print("NULL device keys:", null_device_keys)
print("NULL date keys:", null_date_keys)
print("NULL transaction IDs:", null_transaction_ids)

# =========================================================
# SHOW INVALID FACT RECORDS
# =========================================================

invalid_fact_df = (
    fact_df
    .filter(
        col("customer_key").isNull()
        | col("merchant_key").isNull()
        | col("device_key").isNull()
        | col("date_key").isNull()
        | col("transaction_id").isNull()
    )
)

invalid_count = invalid_fact_df.count()

print("Invalid fact records:", invalid_count)

if invalid_count > 0:

    print("\nInvalid transactions:")

    invalid_fact_df.select(
        "transaction_id",
        "customer_key",
        "merchant_key",
        "device_key",
        "date_key",
        "transaction_timestamp"
    ).show(
        20,
        truncate=False
    )

# =========================================================
# KEEP ONLY VALID FACT RECORDS
# =========================================================

valid_fact_df = (
    fact_df
    .filter(
        col("transaction_id").isNotNull()
        & col("customer_key").isNotNull()
        & col("merchant_key").isNotNull()
        & col("device_key").isNotNull()
        & col("date_key").isNotNull()
    )
    .dropDuplicates(["transaction_id"])
)

valid_fact_count = valid_fact_df.count()

print(
    "Valid fact records to load:",
    valid_fact_count
)

print(
    "Rejected fact records:",
    fact_count - valid_fact_count
)


# =========================================================
# 13. LOAD FACT TABLE
# =========================================================

print("\n======================================")
print("LOADING FACT TABLE")
print("======================================")

valid_fact_df.write.jdbc(
    url=jdbc_url,
    table="fact_transactions",
    mode="append",
    properties=properties
)
print("Fact table loaded successfully.")


# =========================================================
# 14. FINAL SUMMARY
# =========================================================

print("\n======================================")
print("ETL SUMMARY")
print("======================================")

print("Gold transactions:", gold_count)
print("New customers:", new_customer_count)
print("New merchants:", new_merchant_count)
print("New devices:", new_device_count)
print("New dates:", new_date_count)
print("New fact transactions:", valid_fact_count)

print("\n======================================")
print("ETL COMPLETED SUCCESSFULLY!")
print("======================================")


# =========================================================
# 15. STOP SPARK
# =========================================================

spark.stop()
