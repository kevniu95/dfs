#!/bin/sh
docker build -t dfs .
docker run -td --name kniu_dfs dfs python3 load_schedule.py --today

aws ecr get-login-password | docker login --username AWS --password-stdin 541824948902.dkr.ecr.us-east-1.amazonaws.com
docker push 541824948902.dkr.ecr.us-east-1.amazonaws.com/dfs