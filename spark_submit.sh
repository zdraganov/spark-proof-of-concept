#!/bin/bash

docker run --rm -it --link master:master --volumes-from spark-datastore spark-submit:latest spark-submit --master spark://172.17.0.3:7077 /data/$1 
 
