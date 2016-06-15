package com.danosoftware.messaging.kafka;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.iface.IMessagingConsumer;
import com.danosoftware.messaging.iface.IMessagingProcessor;
import com.danosoftware.messaging.iface.IMessagingProducer;
import com.danosoftware.messaging.kafka.utilities.KafkaUtilities;
import com.danosoftware.messaging.kafka.utilities.StringTestUtilities;

public class KafkaMultiThreadTests
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaMultiThreadTests.class);

    // URL for tests
    public static final String URL = "localhost:9092";

    // topic name for tests
    public static final String TOPIC = "testStringTopic";

    // string key name
    private static final String KEY = "testStringKey";

    // group id
    private static final String CONSUMER_GROUP = "testStringGroup";

    @Test
    public void simpleProducerAndConsumerTest() throws InterruptedException
    {
        /*
         *  start Kafka consumer and producer in different threads
         */
        ExecutorService consumerThread = Executors.newSingleThreadExecutor();
        consumerThread.execute(new ConsumerTask(60));

        ExecutorService producerThread = Executors.newSingleThreadExecutor();
        producerThread.execute(new ProducerTask(50));

        /*
         *  wait for producer and consumer to finish
         */
        producerThread.shutdown();
        consumerThread.shutdown();
        producerThread.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        consumerThread.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    /**
     * Consumer inner class that will produce messages for a set time period
     * when executed.
     */
    private class ConsumerTask implements Runnable
    {
        private final IMessagingConsumer consumer;
        private final int seconds;

        private ConsumerTask(int seconds)
        {

            logger.info("Starting-up Consumer. Will run for {} seconds.", seconds);

            // mock string message processor
            @SuppressWarnings("unchecked")
            IMessagingProcessor<String> processor = (IMessagingProcessor<String>) Mockito.mock(IMessagingProcessor.class);

            //  set-up messaging consumer using properties, list of topics and message processor
            this.consumer = KafkaUtilities.kafkaStringConsumer(URL, TOPIC, KEY, CONSUMER_GROUP, processor);

            // time for producer to run in seconds
            this.seconds = seconds;
        }

        /*
         * Consume messages for supplied seconds
         */
        @Override
        public void run()
        {
            // set time when consumer should stop
            long endTime = System.currentTimeMillis() + (seconds * 1000);

            // continuously checks for new messages
            while (System.currentTimeMillis() < endTime)
            {
                consumer.consume();
            }

            consumer.close();
        }
    }

    /**
     * Producer inner class that will produce messages for a set time period
     * when executed.
     */
    private class ProducerTask implements Runnable
    {
        private final IMessagingProducer<String> producer;
        private final int seconds;

        private ProducerTask(int seconds)
        {
            logger.info("Starting Producer. Will run for '{}' seconds.", seconds);

            // set-up producer with properties, topic name and key
            this.producer = KafkaUtilities.kafkaStringProducer(URL, TOPIC, KEY);

            // time for producer to run in seconds
            this.seconds = seconds;
        }

        /*
         * Produce messages for supplied seconds
         */
        @Override
        public void run()
        {
            // set time when producer should stop
            long endTime = System.currentTimeMillis() + (seconds * 1000);

            // continuously checks for new messages
            while (System.currentTimeMillis() < endTime)
            {
                List<String> messages = StringTestUtilities.generateRandomMessages();
                try
                {
                    producer.send(messages);
                    Thread.sleep(5000);
                }
                catch (Exception e)
                {
                    logger.error("Sending messages to Kafka queue failed.", e);
                }
            }

            producer.close();
        }
    }

}
