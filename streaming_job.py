from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, desc

spark = SparkSession.builder \
    .appName("BikeStreaming") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 按10秒滑动窗口统计热门站点
windowed = lines.groupBy(
    window(col("value"), "10 seconds"),
    col("value").alias("station")
).count().orderBy(desc("count"))

query = windowed.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
