#!/bin/sh

docker stop test_f1 && docker rm test_f1
docker build -t f1-dataanalysis . && docker image prune -f
docker run --name test_f1 -p 9090:9090 -d --restart=always f1-dataanalysis