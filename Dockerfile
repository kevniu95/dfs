FROM python:3
WORKDIR /load_schedule
COPY ./load_schedule.py .
COPY ./config.py .
COPY ./config.ini .
COPY ./pgConnect.py .
COPY ./dfs_dao.py .
COPY ./requestLimiter.py .
COPY ./scheduleReader.py .
COPY ./requirements.txt .
RUN pip3 install -r requirements.txt