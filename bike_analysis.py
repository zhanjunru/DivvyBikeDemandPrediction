#!/usr/bin/env python3
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd

plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

spark = SparkSession.builder \
    .appName("BikeRideAnalysis") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. 热门起始站点 Top 10
top_stations = spark.sql("""
    SELECT start_station_name, COUNT(*) as rides
    FROM bike_db.bike_clean
    WHERE start_station_name IS NOT NULL
    GROUP BY start_station_name
    ORDER BY rides DESC
    LIMIT 10
""").toPandas()

plt.figure(figsize=(10,6))
plt.barh(top_stations['start_station_name'], top_stations['rides'], color='skyblue')
plt.xlabel('Number of Rides')
plt.title('Top 10 Start Stations')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig('1_top_start_stations.png')
print("✅ Saved: 1_top_start_stations.png")

# 2. 用户类型分布（饼图）
user_type = spark.sql("""
    SELECT member_casual, COUNT(*) as cnt
    FROM bike_db.bike_clean
    GROUP BY member_casual
""").toPandas()

plt.figure(figsize=(6,6))
plt.pie(user_type['cnt'], labels=user_type['member_casual'], autopct='%1.1f%%', startangle=90)
plt.title('User Type Distribution')
plt.savefig('2_user_type_pie.png')
print("✅ Saved: 2_user_type_pie.png")

# 3. 每小时骑行量分布
hourly = spark.sql("""
    SELECT start_hour, COUNT(*) as rides
    FROM bike_db.bike_clean
    GROUP BY start_hour
    ORDER BY start_hour
""").toPandas()

plt.figure(figsize=(12,5))
plt.bar(hourly['start_hour'], hourly['rides'], color='green', alpha=0.7)
plt.title('Ride Count by Hour of Day')
plt.xlabel('Hour (0-23)')
plt.ylabel('Number of Rides')
plt.xticks(range(0,24))
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig('3_hourly_rides.png')
print("✅ Saved: 3_hourly_rides.png")

# ========== 4. 骑行时长分布（直方图，过滤异常值>120分钟） ==========
duration = spark.sql("""
    SELECT (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / 60.0 AS trip_duration_minutes
    FROM bike_db.bike_clean
    WHERE (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / 60.0 > 0 
      AND (UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / 60.0 <= 120
""").toPandas()

plt.figure(figsize=(10,5))
plt.hist(duration['trip_duration_minutes'], bins=50, color='orange', edgecolor='black', alpha=0.7)
plt.title('Distribution of Trip Duration (0-120 minutes)')
plt.xlabel('Trip Duration (minutes)')
plt.ylabel('Frequency')
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig('4_trip_duration_hist.png')
print("✅ Saved: 4_trip_duration_hist.png")

# 5. 会员 vs 非会员每小时对比
user_hourly = spark.sql("""
    SELECT start_hour, member_casual, COUNT(*) as rides
    FROM bike_db.bike_clean
    GROUP BY start_hour, member_casual
    ORDER BY start_hour, member_casual
""").toPandas()

pivot_hourly = user_hourly.pivot(index='start_hour', columns='member_casual', values='rides').fillna(0)

plt.figure(figsize=(12,5))
plt.plot(pivot_hourly.index, pivot_hourly['casual'], marker='o', label='Casual', color='red')
plt.plot(pivot_hourly.index, pivot_hourly['member'], marker='s', label='Member', color='blue')
plt.title('Hourly Ride Comparison: Casual vs Member')
plt.xlabel('Hour of Day')
plt.ylabel('Number of Rides')
plt.legend()
plt.grid(True, linestyle='--', alpha=0.5)
plt.xticks(range(0,24))
plt.tight_layout()
plt.savefig('5_casual_vs_member_hourly.png')
print("✅ Saved: 5_casual_vs_member_hourly.png")

# 6. 每日骑行量趋势
daily = spark.sql("""
    SELECT start_date, COUNT(*) as rides
    FROM bike_db.bike_clean
    GROUP BY start_date
    ORDER BY start_date
""").toPandas()

plt.figure(figsize=(12,5))
plt.plot(daily['start_date'], daily['rides'], marker='o', linestyle='-', color='purple')
plt.title('Daily Ride Volume Trend')
plt.xlabel('Date')
plt.ylabel('Number of Rides')
plt.xticks(rotation=45)
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig('6_daily_trend.png')
print("✅ Saved: 6_daily_trend.png")

print(f"\nTotal rides: {daily['rides'].sum()}")
print(f"Avg daily rides: {daily['rides'].mean():.1f}")
print(f"Avg trip duration (min): {duration['trip_duration_minutes'].mean():.2f}")

spark.stop()
