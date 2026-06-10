CREATE DATABASE IF NOT EXISTS bike_db;
USE bike_db;

CREATE EXTERNAL TABLE bike_raw_tmp (
    ride_id STRING,
    rideable_type STRING,
    started_at STRING,
    ended_at STRING,
    start_station_name STRING,
    start_station_id STRING,
    end_station_name STRING,
    end_station_id STRING,
    start_lat STRING,
    start_lng STRING,
    end_lat STRING,
    end_lng STRING,
    member_casual STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/user/zjr/bike/input/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE bike_clean (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    start_hour INT,
    start_date STRING,
    start_station_name STRING,
    rideable_type STRING,
    member_casual STRING
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE bike_clean
SELECT
    CAST(started_at AS TIMESTAMP) AS start_time,
    CAST(ended_at AS TIMESTAMP) AS end_time,
    HOUR(CAST(started_at AS TIMESTAMP)) AS start_hour,
    DATE_FORMAT(CAST(started_at AS TIMESTAMP), 'yyyyMMdd') AS start_date,
    start_station_name,
    rideable_type,
    member_casual
FROM bike_raw_tmp
WHERE start_station_name IS NOT NULL
  AND TRIM(start_station_name) != ''
  AND started_at IS NOT NULL
  AND ended_at IS NOT NULL;
