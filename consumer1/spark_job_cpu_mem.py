from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, when, date_format, to_timestamp, round as spark_round, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark
spark = SparkSession.builder \
    .appName("CPU_Memory_Analysis") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print("[INFO] Starting Spark Job 1 - CPU & Memory Analysis")

# Define schemas
cpu_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("cpu_pct", DoubleType(), True)
])

mem_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("mem_pct", DoubleType(), True)
])

# Read CSV files
df_cpu = spark.read.csv("/app/cpu_data.csv", header=True, schema=cpu_schema)
df_mem = spark.read.csv("/app/mem_data.csv", header=True, schema=mem_schema)

df_cpu = df_cpu.withColumn("cpu_pct", col("cpu_pct").cast("double"))
df_mem = df_mem.withColumn("mem_pct", col("mem_pct").cast("double"))

print(f"[INFO] CPU records loaded: {df_cpu.count()}")
print(f"[INFO] Memory records loaded: {df_mem.count()}")

# Show sample data to verify types
print("[INFO] CPU data sample:")
df_cpu.show(5, truncate=False)
df_cpu.printSchema()

print("[INFO] Memory data sample:")
df_mem.show(5, truncate=False)
df_mem.printSchema()

# Convert timestamp
df_cpu = df_cpu.withColumn("timestamp", to_timestamp(col("ts"), "HH:mm:ss"))
df_mem = df_mem.withColumn("timestamp", to_timestamp(col("ts"), "HH:mm:ss"))

# Join CPU and Memory data on timestamp and server_id
df_joined = df_cpu.alias("cpu").join(
    df_mem.alias("mem"),
    (col("cpu.timestamp") == col("mem.timestamp")) & 
    (col("cpu.server_id") == col("mem.server_id")),
    "inner"
).select(
    col("cpu.timestamp").alias("timestamp"),
    col("cpu.server_id").alias("server_id"),
    col("cpu.cpu_pct").alias("cpu_pct"),
    col("mem.mem_pct").alias("mem_pct")
)

print(f"[INFO] Joined records: {df_joined.count()}")
print("[INFO] Joined data sample:")
df_joined.show(5, truncate=False)

# Find minimum timestamp for window alignment
min_timestamp = df_joined.agg(spark_min("timestamp")).collect()[0][0]
print(f"[INFO] Minimum timestamp in data: {min_timestamp}")

# Apply 30-second window with 10-second slide, grouped by server_id
windowed_df = df_joined.groupBy(
    col("server_id"),
    window(col("timestamp"), "30 seconds", "10 seconds")
).agg(
    avg("cpu_pct").alias("avg_cpu_raw"),
    avg("mem_pct").alias("avg_mem_raw")
)

# Round to 2 decimal places using Spark's round
windowed_df = windowed_df.withColumn("avg_cpu", spark_round(col("avg_cpu_raw"), 2)) \
                         .withColumn("avg_mem", spark_round(col("avg_mem_raw"), 2))

# Filter out windows with improper duration (first incomplete window)
windowed_df = windowed_df.withColumn(
    "window_duration_seconds",
    (col("window.end").cast("long") - col("window.start").cast("long"))
)

# Keep only windows with full 30-second duration
windowed_df = windowed_df.filter(col("window_duration_seconds") == 30)

CPU_THRESHOLD = 75.69
MEM_THRESHOLD = 74.28

print(f"[INFO] Using thresholds - CPU: {CPU_THRESHOLD}, MEM: {MEM_THRESHOLD}")

# Apply alerting logic
result_df = windowed_df.withColumn(
    "alert",
    when(
        (col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") > MEM_THRESHOLD),
        "High CPU + Memory stress"
    ).when(
        (col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") <= MEM_THRESHOLD),
        "CPU spike suspected"
    ).when(
        (col("avg_mem") > MEM_THRESHOLD) & (col("avg_cpu") <= CPU_THRESHOLD),
        "Memory saturation suspected"
    ).otherwise("")
)

# Format output with HH:mm:ss timestamps
final_df = result_df.select(
    col("server_id"),
    date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
    date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
    col("avg_cpu"),
    col("avg_mem"),
    col("alert")
).orderBy("server_id", "window_start")

print(f"[INFO] Total windows generated: {final_df.count()}")
final_df.show(truncate=False)

# Write to CSV
final_df.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("/app/team_NO_CPU_MEM_output")

# Rename the part file to final output
import os
import shutil

output_dir = "/app/team_NO_CPU_MEM_output"
output_files = [f for f in os.listdir(output_dir) if f.startswith("part-") and f.endswith(".csv")]

if output_files:
    shutil.move(
        os.path.join(output_dir, output_files[0]),
        "/app/team_NO_CPU_MEM.csv"
    )
    print("[SUCCESS] Output saved to /app/team_NO_CPU_MEM.csv")
else:
    print("[ERROR] No output file found")

spark.stop()
