package com.danosoftware.messaging.kafka.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.iface.IMessagingProducer;
import com.google.common.base.Preconditions;

/**
 * Kafka Messaging Producer is responsible for sending messages to
 * a Kafka topic.
 * 
 * One instance is created for each unique topic and message key.
 * 
 * @author Danny
 *
 * @param <K> - type of message key
 * @param <V> - type of message value
 */
public class KafkaMessagingProducer<K, V> implements IMessagingProducer<V>
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessagingProducer.class);

    // kafka producer
    private final KafkaProducer<K, V> producer;

    // kafka topic name
    private final String topic;

    // kafka message key
    private final K key;

    public KafkaMessagingProducer(Properties props, String topic, K key)
    {
        // confirm all required parameters are supplied
        Preconditions.checkNotNull(props, "null argument passed to KafkaMessagingProducer props parameter");
        Preconditions.checkNotNull(topic, "null argument passed to KafkaMessagingProducer topic parameter");
        Preconditions.checkNotNull(key, "null argument passed to KafkaMessagingProducer key parameter");

        // create a Kafka producer using supplied properties
        this.producer = new KafkaProducer<>(props);

        // Kafka topic and key used by this producer
        this.topic = topic;
        this.key = key;
    }

    @Override
    public void send(List<V> messages) throws InterruptedException, ExecutionException
    {
        for (V aMessage : messages)
        {
            // send message
            ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, key, aMessage);
            producer.send(producerRecord);
        }
        logger.info("Sent '{}' messages.", messages.size());
    }

    @Override
    public void close()
    {
        producer.close();
        logger.info("Closed Producer.");
    }

}
