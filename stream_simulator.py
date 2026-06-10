import socket
import time
import random
from pyspark.sql import SparkSession

# 创建 SparkSession 连接 Hive，获取所有起始站名称
spark = SparkSession.builder \
    .appName("GetStations") \
    .enableHiveSupport() \
    .getOrCreate()

stations_df = spark.sql("SELECT DISTINCT start_station_name FROM bike_db.bike_clean WHERE start_station_name IS NOT NULL")
station_list = [row.start_station_name for row in stations_df.collect()]
spark.stop()

# 如果站点列表为空（极少情况），使用默认列表
if not station_list:
    station_list = ["Station A", "Station B", "Station C"]
    print("警告：未从 Hive 读取到站点，使用默认列表")

# 启动 TCP 服务器，监听本地 9999 端口
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('localhost', 9999))
server.listen(1)
print("模拟器已启动，监听 localhost:9999")
conn, addr = server.accept()
print(f"客户端已连接: {addr}")

try:
    while True:
        # 随机选择一个站点，发送给客户端
        station = random.choice(station_list)
        conn.sendall((station + "\n").encode())
        time.sleep(0.2)   # 每秒发送 5 条数据
except KeyboardInterrupt:
    print("\n模拟器被用户中断")
except Exception as e:
    print(f"错误: {e}")
finally:
    conn.close()
    server.close()
    print("模拟器已关闭")
