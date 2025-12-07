#!/bin/bash

echo "Starting Spark Streaming..."

spark-submit  \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/airflow/weather_risk_assessment/streaming/spark_risk_streaming.py