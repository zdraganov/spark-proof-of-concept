#!/bin/bash 

cd spark-datastore
docker build -t datastore:latest .

cd ../spark-master
docker build -t spark-master:latest .

cd ../spark-slave
docker build -t spark-slave:latest .

cd ../spark-submit
docker build -t spark-submit:latest .
