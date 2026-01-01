#!/bin/bash

echo "Waiting for Kafka to be ready..."
sleep 10

echo "Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic reddit-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic stock-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo "Listing topics..."
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
