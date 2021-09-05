-- Prerequsites
-- replace 'output-bucket-name' to your s3 output-bucket-name

-- create database
CREATE DATABASE nytaxi;


-- create table yellow_taxi that contains the latest snapshot of every day’s yellow taxi data
CREATE EXTERNAL TABLE IF NOT EXISTS nytaxi.yellow_taxi
(
  vendorid int, 
  pickup_datetime timestamp, 
  dropoff_datetime timestamp, 
  passenger_count int, 
  trip_distance double, 
  total_amount double,
  pickup_zone string,
  dropoff_zone string
)
PARTITIONED BY (partition_column string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://output-bucket-name/yellow/';


-- create table green_taxi that contains the latest snapshot of every day’s green taxi data
CREATE EXTERNAL TABLE IF NOT EXISTS nytaxi.green_taxi
(
  vendorid int, 
  pickup_datetime timestamp, 
  dropoff_datetime timestamp, 
  passenger_count int, 
  trip_distance double, 
  total_amount double,
  pickup_zone string,
  dropoff_zone string
)
PARTITIONED BY (partition_column string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://output-bucket-name/green/';


-- create table aggregate_summary that contains the historical snapshot of every day’s aggregated data
CREATE EXTERNAL TABLE nytaxi.aggregate_summary
(
  taxi_type varchar(6), 
  vendorid int, 
  pickup_datetime timestamp, 
  dropoff_datetime timestamp, 
  passenger_count int, 
  trip_distance double, 
  total_amount double, 
  pickup_zone string, 
  dropoff_zone string 
)
PARTITIONED BY (partition_column string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://output-bucket-name/aggregated_summary/'
TBLPROPERTIES ('has_encrypted_data'='false');


-- Examples

-- retrieve 10 sample rows from yellow_taxi table
select * from nytaxi.yellow_taxi limit 10;

-- retrieve 10 sample rows from green_taxi table
select * from nytaxi.green_taxi limit 10;

-- retrieve 10 sample rows from aggregate_summary table
select * from nytaxi.aggregate_summary limit 10;

select * from nytaxi.aggregate_summary where taxi_type='yellow' limit 10;

select * from nytaxi.aggregate_summary where taxi_type='green' limit 10;
