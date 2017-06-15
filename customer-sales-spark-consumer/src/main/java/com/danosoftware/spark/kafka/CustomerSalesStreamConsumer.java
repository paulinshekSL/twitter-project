package com.danosoftware.spark.kafka;

import com.danosoftware.messaging.dto.CustomerSale;
import com.danosoftware.spark.processors.SparkProcessor;
import com.danosoftware.spark.serialization.CustomerSaleDecoder;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CustomerSalesStreamConsumer implements IStreamMessagingConsumer {
	private static final Logger logger = LoggerFactory.getLogger(CustomerSalesStreamConsumer.class);

	// TODO - Danny - replace static constants with parameter service entries
	private static final String SPARK_APP_NAME = "CustomerSalesProcessor";
	private static final String SPARK_MASTER_URL = "local[8]";
	private static final long SPARK_BATCH_DURATION_IN_SECONDS = 2;
	private static final String KAFKA_BROKER_URL = "localhost:9092";

	private final JavaPairInputDStream<String, CustomerSale> kafkaStream;
	private final JavaStreamingContext jssc;
	private final SparkProcessor<String, CustomerSale> processor;

	@Inject
	public CustomerSalesStreamConsumer(@Named("topics") List<String> topics,
			SparkProcessor<String, CustomerSale> processor) {
		// confirm all required parameters are supplied
		Preconditions.checkNotNull(topics, "null argument passed to CustomerSalesStreamConsumer topics parameter");
		Preconditions.checkNotNull(processor,
				"null argument passed to CustomerSalesStreamConsumer processor parameter");

		/*
		 * Create context with specified batch duration interval.
		 *
		 * When creating the Spark configuration, alternative setters can be called depending on use.
		 *
		 * .setAppName(SPARK_APP_NAME) = used when application will be deployed to a cluster.
		 * .setMaster(SPARK_MASTER_URL) = used when testing locally (not deployed to Spark cluster).
		 *
		 */
		SparkConf sparkConf = new SparkConf().setAppName(SPARK_APP_NAME);
		this.jssc = new JavaStreamingContext(sparkConf, Durations.seconds(SPARK_BATCH_DURATION_IN_SECONDS));
		jssc.checkpoint("/tmp/");

		// add Kafka broker url to parameter map
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", KAFKA_BROKER_URL);

		// create set of Kafka topics to consume
		Set<String> topicsSet = new HashSet<String>(topics);

		// initialise Customer Sales DStream from Kafka
		this.kafkaStream = KafkaUtils.createDirectStream(jssc, String.class, CustomerSale.class, StringDecoder.class,
				CustomerSaleDecoder.class, kafkaParams, topicsSet);

		// initialise spark processor
		this.processor = processor;
	}

	@Override
	public void configure() {
		/*
		 * Set-up the spark process. No computation will occur until the stream
		 * starts.
		 */
		processor.process(kafkaStream);
	}

	@Override
	public void close() {
		// close streaming context
		jssc.close();

		logger.info("Closed streaming consumer.");
	}

	@Override
	public void start() {
		// start the computation
		jssc.start();

		logger.info("Started streaming consumer.");

		// block until computation completed or stopped
		jssc.awaitTermination();
	}

	@Override
	public void stop() {
		// stop computation
		jssc.stop();

		logger.info("Stopped streaming consumer.");
	}
}
