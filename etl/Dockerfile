FROM python:3.12-slim-bullseye

WORKDIR /etl

RUN apt-get update && apt-get upgrade && apt-get install -y curl sudo gnupg2

COPY requirements.txt requirements.txt

RUN python3 -m pip install pip --upgrade && pip3 install -r requirements.txt

COPY init.sh init.sh

RUN /bin/bash init.sh

COPY etl.py etl.py

ENTRYPOINT ["python3", "etl.py"]
