FROM python:3.8

RUN pip install confluent_kafka requests websocket-client

COPY kafka-producer.py /kafka-producer.py

CMD ["python", "/kafka-producer.py"]