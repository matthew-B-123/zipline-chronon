#!/bin/bash
#the database step can be commmented out depending on where data is being written
#additionally you woudl need to tweak the running staging query step to point to the correct warehouse
# Get fresh AWS credentials from ~/.aws/credentials
# Update these 3 variables with your current values:
AWS_ACCESS_KEY="PASTE_ACCESS_KEY_HERE"
AWS_SECRET_KEY="PASTE_SECRET_KEY_HERE"
AWS_SESSION_TOKEN="PASTE_SESSION_TOKEN_HERE"

# Create the affirm database first
echo "Creating affirm database..."
docker-compose exec -u root spark-master bash -c "/opt/spark/bin/spark-sql --master local[*] --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.spark_catalog.type=hadoop --conf spark.sql.catalog.spark_catalog.warehouse=file:///tmp/chronon-warehouse --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 -e 'CREATE DATABASE IF NOT EXISTS affirm;'"

# Run the spark-submit command
echo "Running staging query..."
docker-compose exec -u root spark-master bash -c "/opt/spark/bin/spark-submit --master local[*] --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY} --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_KEY} --conf spark.hadoop.fs.s3a.session.token=${AWS_SESSION_TOKEN} --conf spark.hadoop.fs.s3a.path.style.access=false --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.spark_catalog.type=hadoop --conf spark.sql.catalog.spark_catalog.warehouse=file:///tmp/chronon-warehouse --conf spark.sql.parquet.enableVectorizedReader=false --conf spark.sql.parquet.mergeSchema=true --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 --class ai.chronon.spark.Driver /tmp/chronon-spark.jar staging-query-backfill --conf-path /tmp/paybright_repayment_data_v1_0_sq.v1__1 --end-date 2025-01-15"

