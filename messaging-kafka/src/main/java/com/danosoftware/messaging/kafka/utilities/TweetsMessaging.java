package com.danosoftware.messaging.kafka.utilities;

import java.util.Arrays;
import java.util.List;

/**
 * Class that provides static Kafka properties for customer sale messaging
 * 
 * @author Danny
 *
 */
public class TweetsMessaging {
	// topic name for tests
	private static final String TOPIC = "tweetsTopic";

	// string key name
	private static final String KEY = "customerSalesKey";

	// group id
	private static final String CONSUMER_GROUP = "customerSalesGroup";

	// Kafka URL
	private static final String KAFKA_BROKER_URL = "localhost:9092";

	public static String getTopic() {
		return TOPIC;
	}

	public static String getKey() {
		return KEY;
	}

	public static List<String> getTopicList() {
		return Arrays.asList(TOPIC);
	}

	public static String getConsumerGroup() {
		return CONSUMER_GROUP;
	}

	public static String getKafkaUrl() {
		return KAFKA_BROKER_URL;
	}

	// no public construction
	private TweetsMessaging() {

	}

}


