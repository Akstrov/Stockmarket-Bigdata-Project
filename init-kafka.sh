#!/bin/bash

echo "Waiting for Kafka to be ready..."
sleep 5

echo "Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic reddit-data --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic stock-data --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo "Listing all topics..."
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092

echo "Describing topics..."
docker exec kafka kafka-topics --describe --bootstrap-server kafka:9092
