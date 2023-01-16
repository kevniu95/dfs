#!/bin/sh
# docker build -t dfs .
# docker run -td --name dfs_2023 dfs python3 load_schedule.py --year 2023
# sleep 3600
docker run -td --name dfs_2018 dfs python3 load_schedule.py --year 2018
# docker run -td --name dfs_2021 dfs python3 load_schedule.py --year 2021
# docker run -td --name dfs_2020 dfs python3 load_schedule.py --year 2020
# docker run -td --name dfs_2019 dfs python3 load_schedule.py --year 2010
# aws ecr get-login-password | docker login --username AWS --password-stdin 541824948902.dkr.ecr.us-east-1.amazonaws.com
# docker push 541824948902.dkr.ecr.us-east-1.amazonaws.com/dfs