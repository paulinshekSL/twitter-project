package com.danosoftware.spark.kafka;

public interface IStreamMessagingConsumer {

	 /**
     * Configure the messaging consumer.
     */
	void configure();

	 /**
     * Close the messaging consumer.
     */
	void close();

	 /**
     * Start the messaging consumer.
     */
	void start();

	 /**
     * Stop the messaging consumer.
     */
	void stop();
}