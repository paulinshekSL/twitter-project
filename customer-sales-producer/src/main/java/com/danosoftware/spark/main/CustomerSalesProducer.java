package com.danosoftware.spark.main;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import com.danosoftware.messaging.dto.CustomerSale;
import com.danosoftware.messaging.iface.IMessagingProducer;
import com.danosoftware.messaging.kafka.utilities.TweetsMessaging;
import com.danosoftware.messaging.kafka.utilities.KafkaUtilities;

public class CustomerSalesProducer
{

    private static final Logger logger = LoggerFactory.getLogger(CustomerSalesProducer.class);

    public static void main(String[] args) throws InterruptedException
    {
        CustomerSalesProducer spark = new CustomerSalesProducer();
        spark.start(args[0], args[1], args[2], args[3]);
    }

    public void start(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException
    {
        ExecutorService producerThread = Executors.newSingleThreadExecutor();
        producerThread.execute(new ProducerTask(3600, consumerKey, consumerSecret, token, secret));

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
        private final BlockingQueue<String> queue;
        private final StatusesSampleEndpoint endpoint;
        private final Authentication auth;
        private final BasicClient client;

        private ProducerTask(int seconds, String consumerKey, String consumerSecret, String token, String secret)
        {
            logger.info("Starting Producer. Will run for '{}' seconds.", seconds);

            // set-up producer with properties, topic name and key
            this.producer = KafkaUtilities.kafkaCustomerSaleProducer(TweetsMessaging.getKafkaUrl(),TweetsMessaging.getTopic(), TweetsMessaging.getKey());

            // time for producer to run in seconds
            this.seconds = seconds;



            //////////////////////////////////////////////////////

            // Create an appropriately sized blocking queue
            this.queue = new LinkedBlockingQueue<>(10000);

            // Define our endpoint: By default, delimited=length is set (we need this for our processor)
            // and stall warnings are on.
            this.endpoint = new StatusesSampleEndpoint();
            this.endpoint.stallWarnings(false);

            this.auth = new OAuth1(consumerKey, consumerSecret, token, secret);
            //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

            // Create a new BasicClient. By default gzip is enabled.
            this.client = new ClientBuilder()
                    .name("sampleExampleClient")
                    .hosts(Constants.STREAM_HOST)
                    .endpoint(endpoint)
                    .authentication(auth)
                    .processor(new StringDelimitedProcessor(queue))
                    .build();

            // Establish a connection
            this.client.connect();
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
                /// get messages from twitter stream
                List<String> messages = new ArrayList<>();
                for (int msgRead = 0; msgRead < 1000; msgRead++) {
                    if (client.isDone()) {
                        System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                        break;
                    }

                    String msg;
                    try {
                        msg = queue.poll(5, TimeUnit.SECONDS);
                        if (msg == null) {
                            System.out.println("Did not receive a message in 5 seconds");
                        } else {
                            messages.add(msg);
                        }
                    } catch (InterruptedException e) {
                        System.out.println("Thread interrupsed whilst waiting to poll.");
                        break;
                    }
                }

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
