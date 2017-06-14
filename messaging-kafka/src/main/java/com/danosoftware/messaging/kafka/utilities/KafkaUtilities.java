package com.danosoftware.messaging.kafka.utilities;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.danosoftware.messaging.dto.CustomerSale;
import com.danosoftware.messaging.iface.IMessagingConsumer;
import com.danosoftware.messaging.iface.IMessagingProcessor;
import com.danosoftware.messaging.iface.IMessagingProducer;
import com.danosoftware.messaging.kafka.consumer.KafkaMessagingConsumer;
import com.danosoftware.messaging.kafka.producer.KafkaMessagingProducer;
import com.danosoftware.messaging.kafka.serialization.CustomerSaleSerializer;

/**
 * Static methods to support Kafka set-up.
 * 
 * @author Danny
 *
 */
public class KafkaUtilities {
	/**
	 * Return Kafka producer properties for supplied key and value serializers.
	 * 
	 * @return
	 */
	private static Properties producerProperties(String kafkaUrl, Class<?> keySerializer, Class<?> valueSerializer) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());

		return props;
	}

	/**
	 * Return Kafka consumer properties for supplied key and value serializers.
	 * 
	 * @return
	 */
	private static Properties consumerProperties(String kafkaUrl, String consumerGroup, Class<?> keyDeserializer,
			Class<?> valueDeserializer) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());

		return props;
	}

	/**
	 * Return a messaging producer suitable for sending strings
	 * 
	 * @param topic
	 * @param key
	 * @return
	 */
	public static IMessagingProducer<String> kafkaStringProducer(String kafkaUrl, String topic, String key) {
		// get properties when using string keys and values
		Properties props = KafkaUtilities.producerProperties(kafkaUrl, StringSerializer.class, StringSerializer.class);

		// set-up messaging with a Kafka producer, topic name and key
		IMessagingProducer<String> producer = new KafkaMessagingProducer<String, String>(props, topic, key);

		return producer;
	}

	/**
	 * Return a messaging consumer suitable for consuming strings
	 * 
	 * @param topic
	 * @param key
	 * @param consumerGroup
	 * @param processor
	 * @return
	 */
	public static IMessagingConsumer kafkaStringConsumer(String kafkaUrl, String topic, String key,
			String consumerGroup, IMessagingProcessor<String> processor) {
		// get properties when using string keys and values
		Properties props = KafkaUtilities.consumerProperties(kafkaUrl, consumerGroup, StringDeserializer.class,
				StringDeserializer.class);

		// set-up messaging with Kafka consumer, list of topics and supplied
		// message processor
		IMessagingConsumer consumer = new KafkaMessagingConsumer<String, String>(props, Arrays.asList(topic),
				processor);

		return consumer;
	}

	/**
	 * Return a messaging producer suitable for sending customer sales
	 * 
	 * @param topic
	 * @param key
	 * @return
	 */
	public static IMessagingProducer<CustomerSale> kafkaCustomerSaleProducer(String kafkaUrl, String topic,
			String key) {
		// get properties when using string keys and data row document values
		Properties props = KafkaUtilities.producerProperties(kafkaUrl, StringSerializer.class,
				CustomerSaleSerializer.class);

		// set-up messaging with a Kafka producer, topic name and key
		IMessagingProducer<CustomerSale> producer = new KafkaMessagingProducer<String, CustomerSale>(props, topic, key);

		return producer;
	}

}