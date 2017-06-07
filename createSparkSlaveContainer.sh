#!/bin/bash

docker run -d --link master:master --volumes-from spark-datastore spark-slave:latest