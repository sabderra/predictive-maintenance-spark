#!/usr/bin/env bash

DIR=/shared/project

python ${DIR}/kafka/generic_consumer.py localhost:9092 engine-alert
