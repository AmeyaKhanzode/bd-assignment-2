from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, window, when, date_format, to_timestamp, round as spark_round, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark
spark = SparkSession.builder \
    .appName("Network_Disk_Analysis") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print("[INFO] Starting Spark Job 2 - Network & Disk Analysis")

# Define schemas
net_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("net_in", DoubleType(), True),
    StructField("net_out", DoubleType(), True)
])

disk_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("disk_io", DoubleType(), True)
])

# Read CSV files
df_net = spark.read.csv("/app/net_data.csv", header=True, schema=net_schema)
df_disk = spark.read.csv("/app/disk_data.csv", header=True, schema=disk_schema)

df_net = df_net.withColumn("net_in", col("net_in").cast("double")) \
               .withColumn("net_out", col("net_out").cast("double"))
df_disk = df_disk.withColumn("disk_io", col("disk_io").cast("double"))

print(f"[INFO] Network records loaded: {df_net.count()}")
print(f"[INFO] Disk records loaded: {df_disk.count()}")

# Show sample data to verify types
print("[INFO] Network data sample:")
df_net.show(5, truncate=False)
df_net.printSchema()

print("[INFO] Disk data sample:")
df_disk.show(5, truncate=False)
df_disk.printSchema()

# Convert timestamp
df_net = df_net.withColumn("timestamp", to_timestamp(col("ts"), "HH:mm:ss"))
df_disk = df_disk.withColumn("timestamp", to_timestamp(col("ts"), "HH:mm:ss"))

# Join Network and Disk data
df_joined = df_net.alias("net").join(
    df_disk.alias("disk"),
    (col("net.timestamp") == col("disk.timestamp")) & 
    (col("net.server_id") == col("disk.server_id")),
    "inner"
).select(
    col("net.timestamp").alias("timestamp"),
    col("net.server_id").alias("server_id"),
    col("net.net_in").alias("net_in"),
    col("disk.disk_io").alias("disk_io")
)

print(f"[INFO] Joined records: {df_joined.count()}")
print("[INFO] Joined data sample:")
df_joined.show(5, truncate=False)

# Find minimum timestamp
min_timestamp = df_joined.agg(spark_min("timestamp")).collect()[0][0]
print(f"[INFO] Minimum timestamp in data: {min_timestamp}")

# Apply 30-second window with 10-second slide
windowed_df = df_joined.groupBy(
    col("server_id"),
    window(col("timestamp"), "30 seconds", "10 seconds")
).agg(
    spark_max("net_in").alias("max_net_in_raw"),
    spark_max("disk_io").alias("max_disk_io_raw")
)

# Round to 2 decimal places
windowed_df = windowed_df.withColumn("max_net_in", spark_round(col("max_net_in_raw"), 2)) \
                         .withColumn("max_disk_io", spark_round(col("max_disk_io_raw"), 2))

# Filter out windows with improper duration
windowed_df = windowed_df.withColumn(
    "window_duration_seconds",
    (col("window.end").cast("long") - col("window.start").cast("long"))
)

# Keep only windows with full 30-second duration
windowed_df = windowed_df.filter(col("window_duration_seconds") == 30)

# Keep windows starting at or after 20:53:00 to remove initial alignment artifacts
windowed_df = windowed_df.filter(date_format(col("window.start"), "HH:mm:ss") >= "20:53:00")

NET_THRESHOLD = 6477.4
DISK_THRESHOLD = 1176.57

print(f"[INFO] Using thresholds - NET: {NET_THRESHOLD}, DISK: {DISK_THRESHOLD}")

# Apply alerting logic
result_df = windowed_df.withColumn(
    "alert",
    when(
        (col("max_net_in") > NET_THRESHOLD) & (col("max_disk_io") > DISK_THRESHOLD),
        "Network flood + Disk thrash suspected"
    ).when(
        (col("max_net_in") > NET_THRESHOLD) & (col("max_disk_io") <= DISK_THRESHOLD),
        "Possible DDoS"
    ).when(
        (col("max_disk_io") > DISK_THRESHOLD) & (col("max_net_in") <= NET_THRESHOLD),
        "Disk thrash suspected"
    ).otherwise("")
)

# Format output
final_df = result_df.select(
    col("server_id"),
    date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
    date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
    col("max_net_in"),
    col("max_disk_io"),
    col("alert")
).orderBy("server_id", "window_start")

print(f"[INFO] Total windows generated: {final_df.count()}")
final_df.show(truncate=False)

# Write to CSV
final_df.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("/app/team_NO_NET_DISK_output")

# Rename output file
import os
import shutil

output_dir = "/app/team_NO_NET_DISK_output"
output_files = [f for f in os.listdir(output_dir) if f.startswith("part-") and f.endswith(".csv")]

if output_files:
    shutil.move(
        os.path.join(output_dir, output_files[0]),
        "/app/team_NO_NET_DISK.csv"
    )
    print("[SUCCESS] Output saved to /app/team_NO_NET_DISK.csv")
else:
    print("[ERROR] No output file found")

spark.stop()
