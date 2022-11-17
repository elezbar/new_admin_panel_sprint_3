FROM python:3.10

WORKDIR /opt/app

RUN apt-get install gcc

COPY requirements.txt requirements.txt

RUN  pip install -r requirements.txt

COPY . .

ENTRYPOINT ["python", "main.py"]
