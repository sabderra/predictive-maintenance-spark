#!/usr/bin/env bash
port=`docker-compose port kafka 9092 | cut -d: -f2`
host='localhost'
echo "$host:$port"
