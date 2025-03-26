# CCFD
Credit Cards Fraud Detection (CCFD) project aims to identify potential fraud credit card transaction utilising machine learning, and alert the detected transaction on a graph.

This project is a project based learning, utilising: 
- Pyspark
- Pyspark MLlib
- Kafka
- PostgreSQL
- Grafana
- Docker

**Data set used for this project**
- https://www.kaggle.com/datasets/kartik2112/fraud-detection?resource=download

## Features
- **Data Processing**: Cleans, transforms, and prepares data for analysis.
- **Machine Learning**: Train LogisticRegression, RandomForest, and GradientBoostTree Model, with the processed data and select the best performed model.
- **Realtime Simulation**: Simulate realtime data log with Kafka.
- **Fraud Detection**: Applied the model on realtime data and stored the fraud transaction in PostgreSQL database.
- **Alert System**: Plotting a graph of fraud transaction with grafana as an alert system.

## Requirement
- Docker/ Docker Desktop
- VSCode
- Python

##Installation

Jupyter/Pyspark-notebook Docker Container setup:
```bash
docker build -t pyspark_env .
docker run -it -p 8888:8888 -v ${pwd}:/app --name pyspark_env pyspark_env
```

Grafana, PostgreSQL Docker Container setup:
```bash
docker network create fraud-detection-network
docker-compose -f Database/docker-compose.yml up -d
```

Kafka, Pysaprk Docker Container setup: (if producer-1 container does not start, manually start it again, wait around 10 seconds and it will works)
```bash
docker-compose -f kafka/docker-compose.yml up -d
```

Check credit-card-transactions topic in Kafka:
```bash
docker exec -it kafka-kafka-1 /bin/bash
```
if there is no credit-card-transactions:
```bash
kafka-topics --create --topic credit-card-transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --list --bootstrap-server localhost:9092
```
Manually add PostgreSQL datasource to grafana:
- insert photo later

Creating graph
- insert photo later
