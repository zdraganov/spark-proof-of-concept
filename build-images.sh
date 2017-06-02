#!/bin/bash

cd consumer
docker build -t consumer_test:latest .

cd ../producer
docker build -t producer_test:latest .

cd ../mongodb
docker build -t mongodb .

# cd ../spark
# docker build -t spark-consumer .

cd ..
