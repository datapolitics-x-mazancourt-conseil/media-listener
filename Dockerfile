# syntax=docker/dockerfile:1
FROM python:3.9.7-slim-bullseye

RUN apt-get update
RUN apt-get install -y git

# Prépare le planification
RUN apt-get install -y cron
COPY schedule.cron /etc/cron.d/schedule.cron
RUN chmod 0644 /etc/cron.d/schedule.cron && crontab /etc/cron.d/schedule.cron

# installe les dépendances externes
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# copie le code 
COPY . .

# lance la génération des analyses
RUN chmod +x exec.sh
CMD ["sh", "start.sh"]
