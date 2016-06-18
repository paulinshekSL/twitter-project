package com.danosoftware.spark.main;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.beans.CustomerSale;
import com.danosoftware.spark.constants.CustomerSaleMessaging;
import com.danosoftware.spark.kafka.CustomerSalesStreamConsumer;
import com.danosoftware.spark.kafka.IStreamMessagingConsumer;
import com.danosoftware.spark.processors.CustomerSaleStream;
import com.danosoftware.spark.processors.SparkProcessor;

/**
 * Creates a Consumer for a fixed amount of time.
 * Data is consumed in batches.
 * The consumer will process any received data using Spark Streaming.
 * 
 * After the timers expire, the consumer is shut-down. It is recommended
 * that the consumer has a longer timeout than the producer to allow all data to be consumed.
 * 
 * @author Danny
 *
 */
public class CustomerSalesConsumer
{

    private static final Logger logger = LoggerFactory.getLogger(CustomerSalesConsumer.class);

    public static void main(String[] args) throws InterruptedException
    {
        CustomerSalesConsumer spark = new CustomerSalesConsumer();
        spark.start();
    }

    public void start() throws InterruptedException
    {
        SparkProcessor<String, CustomerSale> processor = new CustomerSaleStream();
        IStreamMessagingConsumer consumer = new CustomerSalesStreamConsumer(CustomerSaleMessaging.getTopicList(), processor);

        /*
         *  start Kafka consumer.
         *  If the consumer stops before all data has been sent/consumed - an exception may be thrown.
         */
        ExecutorService consumerThread = Executors.newSingleThreadExecutor();
        consumerThread.execute(new ConsumerTask(consumer, 3600));

        /*
         *  wait for consumer to finish
         */
        consumerThread.shutdown();
        consumerThread.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        logger.info("Timeout expired. Consumer forcibly shutdown.");
    }

    /**
     * Consumer inner class that will produce messages for a set time period
     * when executed.
     */
    private class ConsumerTask implements Runnable
    {
        private final IStreamMessagingConsumer consumer;
        private final int seconds;

        private ConsumerTask(IStreamMessagingConsumer consumer, int seconds)
        {
            logger.info("Starting-up Consumer. Will run for {} seconds.", seconds);

            this.consumer = consumer;

            // time for consumer to run in seconds
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

            // configure the streaming consumer for processing
            consumer.configure();

            /*
             * Start consumer in a separate thread since starting the consumer
             * will block this thread until completed.
             */
            ExecutorService producerThread = Executors.newSingleThreadExecutor();
            producerThread.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    consumer.start();
                }
            });

            // wait until consumer time has elapsed
            while (System.currentTimeMillis() < endTime)
            {
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException e)
                {
                    logger.error("InterruptedException error.", e);
                }
            }

            // shut-down consumer
            consumer.stop();
            consumer.close();
        }
    }
}
