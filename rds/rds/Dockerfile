FROM python:3
WORKDIR /load_schedule
RUN mkdir ./data
RUN mkdir ./data/rl
RUN mkdir ./data/rl/log
COPY ./bs4utils.py .
COPY ./config.ini .
COPY ./config.py .
COPY ./dfs_dao.py .
COPY ./load_schedule.py .
COPY ./pgConnect.py .
COPY ./requestLimiter.py .
COPY ./requirements.txt .
COPY ./scheduleReader.py .
RUN pip3 install -r requirements.txt