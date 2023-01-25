#!/bin/bash
docker build --platform linux/amd64 -t dfs .
# docker run -td --name dfs_2023 dfs python3 load_schedule.py --year 2023
aws ecr get-login-password | docker login --username AWS --password-stdin 541824948902.dkr.ecr.us-east-1.amazonaws.com
docker tag dfs 541824948902.dkr.ecr.us-east-1.amazonaws.com/kniu:dfs
docker push 541824948902.dkr.ecr.us-east-1.amazonaws.com/kniu:dfs

