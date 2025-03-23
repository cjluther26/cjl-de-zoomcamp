 `05-streaming` Notes

## 6.1 - Introduction to Streaming

## 6.2 - What is Stream Processing

## 6.3 - What is Kafka?

### Basics
- Topic: continuous stream of events, where each event is a single data point (which can be represented as almost anything).
- Logs: how we store events in a topic
- Event
    - each event carries a message, which generally have a **key-value-timestamp** structure.
- Producers: produces data / events to a given topic
- Consumers: *consume* data from the topic

### What Kafka Offers (Over Comparable Tools)
- Robustness: topic reliability and replication (maintains functionality even in server-down events).
- Scalable & flexible: top-down scalability, plug-in flexibility.

Kafka allows events/data to be written to topics that consumers (microservices) to *subscribe to*, which helps streamline data transfer and minimizes the transport of excess data to locations in which it isn't needed. (CDC - change data capture, part of Kafka Connect)


## 6.4 - Confluent Cloud

## 6.5 - Kafka Producer Consumer

## 6.6 - Kafka Configuration

## 6.7 - Kafka Stream Basics

## 6.8 - Kafka Stream Join

## 6.9 - Kafka Stream Testing

## 6.10 - Kafka Stream Windowing

## 6.11 - Kafka ksqldb & Connect

## 6.12 - Kafka Schema Registry

## 6.13 - Kafka Streaming with Python

## 6.14 - PySpark Structured Streaming