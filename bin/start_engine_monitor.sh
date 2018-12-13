#!/usr/bin/env bash

RLU_THRESHOLD=$1

DIR=/shared/project

spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --jars ${DIR}/jars/spark-streaming-kafka-0-10_2.11-2.4.0.jar ${DIR}/kafka/engine_cycle_consumer_struct.py localhost:9092 engine-stream engine-alert -w 30 -s 30 -r ${RLU_THRESHOLD} 
