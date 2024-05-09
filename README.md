# Traffic Data Monitoring using Spark Streaming

## Introduction
The Traffic Data Monitoring project leverages real-time IoT data from connected vehicles to monitor traffic on different routes. It consists of three standalone Maven applications written in Java: IoT Data Producer, IoT Data Processor, and IoT Data Dashboard. Each module serves a specific purpose in processing and presenting traffic data for analysis and visualization. The project utilizes Apache Kafka for message streaming, Spark Streaming for data processing, Cassandra for data storage, and Spring Boot for web application development.

## Modules

### 1. IoT Data Producer
The IoT Data Producer is responsible for simulating connected vehicles and generating IoT messages. These messages are captured by a message broker (Apache Kafka) and sent to the streaming application for processing.

### 2. IoT Data Processor
The IoT Data Processor is a Spark Streaming application that consumes IoT data streams and analyzes them for traffic data. It provides the following metrics:
- Total vehicle count for different types of vehicles on various routes, stored in a Cassandra database.
- Vehicle count for the last 30 seconds window, categorized by vehicle types and routes, stored in the Cassandra database.
- Details of vehicles within the radius of a given Point of Interest (POI), stored in the Cassandra database.

### 3. IoT Data Dashboard
The IoT Data Dashboard is a Spring Boot application responsible for retrieving data from the Cassandra database and presenting it on a web page. It utilizes Web Sockets and jQuery to push data to the web page at fixed intervals, ensuring automatic data refresh. The dashboard displays data in charts and tables, making use of responsive web design with Bootstrap.js for accessibility on both desktop and mobile devices.

## Technologies Used
- JDK - 1.8
- Maven - 3.3.9
- ZooKeeper - 3.4.8
- Kafka - 2.10-0.10.0.0
- Cassandra - 2.2.6
- Spark - 1.6.2 Pre-built for Hadoop 2.6
- Spring Boot - 1.3.5
- jQuery.js
- Bootstrap.js
- Sockjs.js
- Stomp.js
- Chart.js

## Project Structure
The Traffic Data Monitoring project is a Maven Aggregator project comprising the following three projects:
1. IoT Kafka Producer
2. IoT Spark Processor
3. IoT Spring Boot Dashboard

## Acknowledgments
- The developers and contributors of Apache Kafka, Spark, Cassandra, and Spring Boot for providing powerful tools and frameworks for real-time data processing and web application development.
- Open-source community for their valuable contributions and support.
---

The Traffic Data Monitoring project aims to provide real-time insights into traffic patterns using IoT data from connected vehicles. With its modular design and use of cutting-edge technologies, it offers a scalable and efficient solution for monitoring and analyzing traffic data.
