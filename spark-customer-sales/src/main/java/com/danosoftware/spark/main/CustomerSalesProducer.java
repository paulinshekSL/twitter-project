package com.danosoftware.spark.main;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.beans.CustomerSale;
import com.danosoftware.messaging.iface.IMessagingProducer;
import com.danosoftware.messaging.kafka.utilities.KafkaUtilities;
import com.danosoftware.spark.constants.CustomerSaleMessaging;
import com.danosoftware.spark.utilities.CustomerSalesUtilities;

public class CustomerSalesProducer
{

    private static final Logger logger = LoggerFactory.getLogger(CustomerSalesProducer.class);

    public static void main(String[] args) throws InterruptedException
    {
        CustomerSalesProducer spark = new CustomerSalesProducer();
        spark.start();
    }

    public void start() throws InterruptedException
    {
        ExecutorService producerThread = Executors.newSingleThreadExecutor();
        producerThread.execute(new ProducerTask(3600));

        /*
         *  wait for producer and consumer to finish
         */
        producerThread.shutdown();
        producerThread.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    /**
     * Producer inner class that will produce messages for a set time period
     * when executed.
     */
    private class ProducerTask implements Runnable
    {
        private final IMessagingProducer<CustomerSale> producer;
        private final int seconds;

        private ProducerTask(int seconds)
        {
            logger.info("Starting Producer. Will run for '{}' seconds.", seconds);

            // set-up producer with properties, topic name and key
            this.producer = KafkaUtilities.kafkaCustomerSaleProducer(CustomerSaleMessaging.getKafkaUrl(),CustomerSaleMessaging.getTopic(), CustomerSaleMessaging.getKey());

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
                List<CustomerSale> messages = CustomerSalesUtilities.generateCustomerSales();

                try
                {
                    producer.send(messages);
                    Thread.sleep(1000);
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
