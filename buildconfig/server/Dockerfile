FROM python:3.10-slim-buster

RUN apt update
RUN apt install cron -y
RUN apt install curl -y
RUN pip install --upgrade pip
WORKDIR /server
COPY buildconfig/server/sysconf sysconf
COPY src src
RUN pip install -r sysconf/requirements.txt
RUN crontab sysconf/crontab
EXPOSE 8080
EXPOSE 9000

CMD ["/bin/bash", "-c", "'/etc/init.d/cron start; python /server/src/main.py; exit 0;'"]
