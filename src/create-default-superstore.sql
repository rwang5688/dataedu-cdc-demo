CREATE EXTERNAL TABLE IF NOT EXISTS "default"."superstore" ( 
row_id STRING,
order_id STRING,
order_date STRING,
ship_date STRING,
ship_mode STRING,
customer_id STRING,
customer_name STRING,
segment STRING,
country STRING,
city STRING,
state STRING,
postal_code STRING,
region STRING,
product_id STRING,
category STRING,
sub_category STRING,
product_name STRING,
sales STRING,
quantity STRING,
discount STRING,
profit STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://glue-delta-lake-demo-us-west-2-3f8a6345c81e4d5b8e88f3d8f318a3c4/delta/current/_symlink_format_manifest/'
