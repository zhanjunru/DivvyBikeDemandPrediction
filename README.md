# 🚲 Divvy共享单车出行行为分析与需求量预测

基于 Hadoop + Spark + Hive 的芝加哥 Divvy 共享单车大数据分析系统，涵盖数据清洗、多维度可视化、时间序列预测与实时流模拟。

## 📊 项目简介

本项目以芝加哥 Divvy 共享单车 2025 年 1 月的真实骑行数据为对象，利用 Hadoop 生态技术实现：

- **离线分析**：6 张可视化图表，展示热门站点、用户类型、时段分布、骑行时长、会员对比、每日趋势。
- **需求预测**：基于滑动窗口（前 6 小时）和随机森林回归，预测热门站点的下一小时借车量。
- **实时流模拟**：模拟骑行事件流，以 10 秒为窗口统计实时热门站点。

## 📂 数据集

- **来源**：[芝加哥 Divvy 公开数据](https://divvy-tripdata.s3.amazonaws.com/index.html)
- **时间范围**：2025 年 1 月
- **记录数**：115,837 条
- **大小**：28.6 MB（原始 CSV）
- **核心字段**：`started_at`, `ended_at`, `start_station_name`, `member_casual`, `rideable_type`

## 🛠️ 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Hadoop | 3.3.6 | HDFS 存储、YARN 资源管理 |
| Hive | 3.1.2 | 数据清洗与数据仓库 |
| Spark | 3.2.0 | SQL 分析、MLlib 预测、Structured Streaming |
| Python | 3.8 | 可视化（Matplotlib）、脚本编写 |
| MySQL | 8.0 | Hive Metastore 元数据存储 |

## 📈 离线分析结果（6 张图表）

| 图表 | 说明 |
|------|------|
| `1_top_start_stations.png` | 热门起始站点 Top 10 |
| `2_user_type_pie.png` | 会员 vs 临时用户占比 |
| `3_hourly_rides.png` | 全天各小时骑行量分布 |
| `4_trip_duration_hist.png` | 骑行时长直方图（0-120 分钟） |
| `5_casual_vs_member_hourly.png` | 两类用户分时段对比 |
| `6_daily_trend.png` | 每日骑行量趋势 |

**关键统计**：
- 总骑行次数：115,837
- 日均骑行量：3,619.9
- 平均骑行时长：9.47 分钟
- 最热门站点：**Kingsbury St & Kinzie St**（1,798 次）

## 🔮 时间序列预测

- **目标站点**：Kingsbury St & Kinzie St
- **特征**：前 6 小时的借车量
- **模型**：随机森林回归（`numTrees=50`, `maxDepth=10`）
- **评估指标**：
  - RMSE = 4.07
  - R² = 0.0928

> 注：R² 偏低可能由于该站点数据波动较大或特征不足，未来可加入天气、节假日等特征优化。

## 🌊 实时流模拟（可选）

- **模拟器**（`stream_simulator.py`）：从 Hive 读取所有站点，随机发送到 localhost:9999。
- **流作业**（`streaming_job.py`）：使用 Spark Structured Streaming，每 10 秒统计一次窗口内的热门站点。
- **手动测试**（如资源不足）：
  ```bash
  nc -lk 9999
  # 在另一终端提交 streaming_job.py，然后在 nc 中输入站点名
📁 文件清单
文件	描述
bike_create_table.hql	Hive 建表与数据清洗脚本
bike_analysis.py	Spark SQL 多维分析 + 可视化
bike_prediction.py	随机森林时间序列预测
stream_simulator.py	实时数据模拟器
streaming_job.py	流处理作业
station_list.txt	所有站点名称列表（供模拟器使用）
*.png	生成的 6 张分析图表
🚀 快速运行（在 Ubuntu 虚拟机中）
1. 启动环境
bash
start-dfs.sh
start-yarn.sh
nohup hive --service metastore &
2. 数据清洗
bash
hive -f bike_create_table.hql
3. 离线分析
bash
spark-submit --master local[*] --jars /usr/local/hive/lib/mysql-connector-j-8.0.33.jar bike_analysis.py
4. 时间序列预测
bash
# 修改 bike_prediction.py 中的 target_station 为实际热门站点
spark-submit --master local[*] --jars /usr/local/hive/lib/mysql-connector-j-8.0.33.jar bike_prediction.py
5. 实时流模拟（两个终端）
bash
# 终端1
spark-submit --master local[*] --jars /usr/local/hive/lib/mysql-connector-j-8.0.33.jar stream_simulator.py

# 终端2
spark-submit --master local[*] --jars /usr/local/hive/lib/mysql-connector-j-8.0.33.jar streaming_job.py
⚠️ 常见问题及解决
问题	解决方法
Hive Metastore 连接 MySQL 失败	将 mysql-connector-java.jar 放入 /usr/local/hive/lib
Spark 作业找不到 Hive 表	复制 hive-site.xml 到 $SPARK_HOME/conf/
ModuleNotFoundError: No module named 'matplotlib'	pip install matplotlib pandas
流模拟时端口 9999 被占用	更换端口，并同步修改两个脚本中的端口号
📜 实验报告与成果
项目已完全在 Ubuntu 虚拟机单节点 Hadoop 环境上运行通过。

完整代码已开源在 GitHub：https://github.com/zhanjunru/DivvyBikeDemandPrediction

📄 许可证
MIT License
