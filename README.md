# Real-Time Time Series Data Prediction

## Overview
This project is currently in progress and combines Apache Flink, Kafka, and deep learning to analyze and predict trends in time series data. The goal is to dynamically integrate live data for immediate and accurate forecasting, advancing real-time data analysis.

## Methodology
The methodology involves setting up a Docker environment and implementing data streaming and processing components:

1. **Docker Environment Setup:** Using Docker Compose to configure services including Zookeeper, Kafka, Flink Job Manager, Flink Task Manager, and a custom Flink job.
2. **Apache Flink Integration:** Implementing a Flink job (`flink_job.py`) that reads and processes data streams. 
3. **Kafka Source and Sink Configuration:** Setting up Kafka as both the source and sink for data streams, allowing for robust data ingestion and output.
4. **Deep Learning Model Research:** Exploring and implementing deep learning models for time series prediction.

## Key Components
- **Docker Compose:** A `docker-compose.yml` file to orchestrate multi-container Docker applications.
- **Flink Job Script (`flink_job.py`):** Python script for data stream processing using Apache Flink.
- **Kafka Connector (`kafka-connector.py`):** Python code to manage Kafka source and sink connections.
- **Dockerfile Configuration:** Building a Docker image with necessary dependencies and environment for the Flink job.

## Evaluation
Evaluation will include:
- **Real-time Data Processing:** Testing the efficiency of data processing in real-time scenarios.
- **Prediction Accuracy:** Evaluating the accuracy of trend predictions made by deep learning models.
- **Scalability and Performance:** Assessing the system's scalability and performance under different loads.

## Conclusion
The project aims to showcase the integration of stream processing and machine learning for real-time data analysis and trend prediction, utilizing the strengths of Apache Flink and Kafka in processing large volumes of data efficiently.
