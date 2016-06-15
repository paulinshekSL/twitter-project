package com.danosoftware.messaging.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.iface.IMessagingConsumer;
import com.danosoftware.messaging.iface.IMessagingProcessor;
import com.google.common.base.Preconditions;

/**
 * Kafka Messaging Consumer is responsible for consuming messages from
 * a Kafka topic and passing it on to the registered IMessagingProcessor
 * for processing.
 * 
 * @author Danny
 *
 * @param <K> - type of message key
 * @param <V> - type of message value
 */
public class KafkaMessagingConsumer<K, V> implements IMessagingConsumer
{

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessagingConsumer.class);

    // kafka consumer
    private final KafkaConsumer<K, V> consumer;

    // registered processor - messages will be sent to the processor for processing
    private final IMessagingProcessor<V> processor;

    public KafkaMessagingConsumer(Properties props, List<String> topics, IMessagingProcessor<V> processor)
    {
        // confirm all required parameters are supplied
        Preconditions.checkNotNull(props, "null argument passed to KafkaMessagingConsumer props parameter");
        Preconditions.checkNotNull(topics, "null argument passed to KafkaMessagingConsumer topics parameter");
        Preconditions.checkNotNull(processor, "null argument passed to KafkaMessagingConsumer processor parameter");

        // create a Kafka consumer that uses string keys and values
        this.consumer = new KafkaConsumer<K, V>(props);

        // subscribe to the supplied topics
        consumer.subscribe(topics);

        // processor for any consumed messages
        this.processor = processor;
    }

    @Override
    public void consume()
    {
        // list of messages consumed
        List<V> messages = new ArrayList<>();

        // poll for new messages
        ConsumerRecords<K, V> records = consumer.poll(100);

        /*
         * Add received messages to list and send to registered processor
         */
        if (records.count() > 0)
        {
            logger.info("Recieved {} messages.", records.count());

            for (ConsumerRecord<K, V> record : records)
            {
                logger.trace("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                messages.add(record.value());
            }

            // send list of messages to target processor
            processor.process(messages);
        }
    }

    @Override
    public void close()
    {
        consumer.close();
        logger.info("Closed Consumer.");

    }
}
