#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, row_number, concat, lpad, to_timestamp
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("BikeDemandPrediction") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

target_station = "Kingsbury St & Kinzie St"
print(f"正在分析站点: {target_station}")

# 1. 按小时聚合
hourly_demand = spark.sql(f"""
    SELECT 
        start_date,
        start_hour,
        COUNT(*) AS demand
    FROM bike_db.bike_clean
    WHERE start_station_name = '{target_station}'
    GROUP BY start_date, start_hour
    ORDER BY start_date, start_hour
""")

# 2. 构建时间字符串（例如 '20250101 08'）并转时间戳，用于排序
# 注意：lpad 将小时补齐2位
hourly_ts = hourly_demand.withColumn(
    "datetime_str",
    to_timestamp(
        concat(col("start_date"), lpad(col("start_hour").cast("string"), 2, "0")),
        "yyyyMMddHH"
    )
)
window_spec = Window.orderBy("datetime_str")
hourly_ts = hourly_ts.withColumn("row_id", row_number().over(window_spec))

# 3. 滞后特征
for i in range(1, 7):
    hourly_ts = hourly_ts.withColumn(f"lag_{i}", lag("demand", i).over(window_spec))

model_df = hourly_ts.na.drop()

# 4. 特征组装
feature_cols = [f"lag_{i}" for i in range(1, 7)]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data = assembler.transform(model_df).select("features", col("demand").alias("label"))

# 5. 划分训练/测试
total = data.count()
train = data.limit(int(total * 0.8))
test = data.subtract(train)
print(f"总样本数: {total}, 训练集: {train.count()}, 测试集: {test.count()}")

# 6. 随机森林
rf = RandomForestRegressor(numTrees=50, maxDepth=10, seed=42)
model = rf.fit(train)

# 7. 预测评估
predictions = model.transform(test)
rmse = RegressionEvaluator(labelCol="label", metricName="rmse").evaluate(predictions)
r2 = RegressionEvaluator(labelCol="label", metricName="r2").evaluate(predictions)

print(f"\n=== 预测结果 for station: {target_station} ===")
print(f"RMSE: {rmse:.2f}")
print(f"R²: {r2:.4f}")

print("\n真实值 vs 预测值 (前20行):")
predictions.select("label", "prediction").show(20, truncate=False)

spark.stop()
