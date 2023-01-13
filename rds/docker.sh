#!/bin/sh
docker build -t dfs .
docker run -td --name dfs_2022 dfs python3 load_schedule.py --year 2022

aws ecr get-login-password | docker login --username AWS --password-stdin 541824948902.dkr.ecr.us-east-1.amazonaws.com
docker push 541824948902.dkr.ecr.us-east-1.amazonaws.com/dfs