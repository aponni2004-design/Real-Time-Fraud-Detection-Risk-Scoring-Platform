from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("FraudDetectionKafkaStream")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("===================================")
print("PySpark Kafka Streaming Test")
print("Spark Version:", spark.version)
print("===================================")


df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .load()
)

print("Kafka connection created successfully!")

query = (
    df.selectExpr("CAST(value AS STRING) AS transaction")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

