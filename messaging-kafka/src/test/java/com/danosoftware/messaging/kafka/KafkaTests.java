package com.danosoftware.messaging.kafka;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.iface.IMessagingConsumer;
import com.danosoftware.messaging.iface.IMessagingProcessor;
import com.danosoftware.messaging.iface.IMessagingProducer;
import com.danosoftware.messaging.iface.IStream;
import com.danosoftware.messaging.kafka.utilities.KafkaUtilities;
import com.danosoftware.messaging.kafka.utilities.StringTestUtilities;

import junit.framework.Assert;

public class KafkaTests
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaTests.class);

    // URL for tests
    public static final String URL = "localhost:9092";

    // topic name for tests
    public static final String TOPIC = "testStringTopic";

    // string key name
    private static final String KEY = "testStringKey";

    // group id
    private static final String CONSUMER_GROUP = "testStringGroup";

    @Test
    public void testProducerLifecycle()
    {
        // set-up messaging producer suitable for sending strings
        IMessagingProducer<String> producer = KafkaUtilities.kafkaStringProducer(URL, TOPIC, KEY);

        // get a list of messages to send
        List<String> messages = StringTestUtilities.generateRandomMessages();

        // send messages to messaging producer
        try
        {
            producer.send(messages);
        }
        catch (Exception e)
        {
            logger.error("Sending messages to Kafka queue failed.", e);
        }

        // close
        producer.close();
    }

    @Test
    public void testConsumerLifecycle()
    {
        /*
         * Mocked string message processor
         */
        @SuppressWarnings("unchecked")
        IMessagingProcessor<String> processor = (IMessagingProcessor<String>) Mockito.mock(IMessagingProcessor.class);

        // set-up messaging consumer suitable for consuming strings
        IMessagingConsumer consumer = KafkaUtilities.kafkaStringConsumer(URL, TOPIC, KEY, CONSUMER_GROUP, processor);

        //consume message
        consumer.consume();

        //close
        consumer.close();
    }

    /*
     * Test sending and consuming a set number of messages.
     * Test ensures the number of messages received matches the number sent
     */
    @Test
    public void testProducedMessagesConsumed() throws InterruptedException
    {
        /*
         * Anonymous Processor Class. Adds received messages to a consumed messages list.
         * This should be called whenever the consumer receives new messages.
         */
        final List<String> consumedMessages = new ArrayList<>();
        IMessagingProcessor<String> processor = new IMessagingProcessor<String>()
        {
            @Override
            public void process(List<String> messages)
            {
                consumedMessages.addAll(messages);
            }

            @Override
            public void initialiseOutputStream(IStream istream)
            {
                // TODO Auto-generated method stub

            }
        };

        // set-up messaging consumer suitable for consuming strings
        IMessagingConsumer consumer = KafkaUtilities.kafkaStringConsumer(URL, TOPIC, KEY, CONSUMER_GROUP, processor);

        // set-up messaging producer suitable for sending strings
        IMessagingProducer<String> producer = KafkaUtilities.kafkaStringProducer(URL, TOPIC, KEY);

        // get a list of messages to send
        List<String> messages = StringTestUtilities.generateRandomMessages();

        // send messages to messaging producer
        try
        {
            producer.send(messages);
        }
        catch (Exception e)
        {
            logger.error("Sending messages to Kafka queue failed.", e);
        }

        /*
         * Wait until all messages have been received by the consumer
         */
        while (consumedMessages.size() < messages.size())
        {
            Thread.sleep(1000);
            consumer.consume();
        }

        producer.close();
        consumer.close();

        Assert.assertEquals(messages.size(), consumedMessages.size());
    }
}