#!/bin/bash

# This script expects the APACHE_KAFKA_DIR environmental variable to have
# been set to the directory where Kafka is deployed to. This is used to find
# the needed Kafka scripts.
#
# Alternatively the APACHE_KAFKA_DIR variable can be hard-coded here. For example:
# APACHE_KAFKA_DIR="/home/user1/my-apps/kafka_2.11-0.9.0.0"

if [ -z "$APACHE_KAFKA_DIR" ] ; then
        "echo WARNING - APACHE_KAFKA_DIR NOT SET. CAN NOT START KAFKA"
else

    if [ ! -d "$APACHE_KAFKA_DIR" ]; then
        echo -e "FATAL: Could not find Apache Kafka at: \t\t\t\t${APACHE_KAFKA_DIR}"
        echo
        exit 1
    else
        echo -e "INFO: Apache Kafka installed to: \t\t\t\t${APACHE_KAFKA_DIR}"
    fi

	start_zookeeper_command="${APACHE_KAFKA_DIR}/bin/zookeeper-server-start.sh ${APACHE_KAFKA_DIR}/config/zookeeper.properties &"
	start_kafka_command="${APACHE_KAFKA_DIR}/bin/kafka-server-start.sh ${APACHE_KAFKA_DIR}/config/server.properties &"
 
    # If starting up - Zookeeper should start before Kafka.
    echo "Starting Zookeeper then Kafka...."
    echo ${start_zookeeper_command}
    eval ${start_zookeeper_command}

    echo ${start_kafka_command}
    eval ${start_kafka_command}

    # Create Kafka topic if it doesn't already exist
    wantedTopic="customerSalesTopic"
    topicExists=`${APACHE_KAFKA_DIR}/bin/kafka-topics.sh --list --zookeeper localhost:2181 | grep ${wantedTopic}`
    if [ -z "$topicExists" ]; then
            echo -e "WARN: Could not find Kafka Topic: ${wantedTopic}. Creating it..."
            ${APACHE_KAFKA_DIR}/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ${wantedTopic}
        else
            echo -e "INFO: Found Kafka Topic: ${wantedTopic}"
    fi
fi
