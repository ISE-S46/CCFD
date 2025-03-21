Data set used for this project:
- https://www.kaggle.com/datasets/kartik2112/fraud-detection?resource=download

Jupyter/Pyspark-notebook Docker Container setup(Window):
- docker build -t pyspark_env .
- docker run -it -p 8888:8888 -v ${pwd}:/app --name pyspark_env pyspark_env

Kafka, Pysaprk Docker Container setup(Window):
- docker-compose -f kafka/docker-compose.yml up -d

Create credit-card-transactions topic in Kafka
- docker exec -it kafka-kafka-1 /bin/bash
- kafka-topics --create --topic credit-card-transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
- kafka-topics --list --bootstrap-server localhost:9092

Grafana, PostgreSQL Docker Container setup(Window):
- docker-compose -f Database/docker-compose.yml up -d
