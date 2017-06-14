package com.danosoftware.spark.processors;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.dto.CustomerSale;
import com.danosoftware.spark.utilities.FileUtilities;

import scala.Tuple2;

/**
 * Spark processor that calculates the average value per unique enumerations
 * seen in the stream.
 * 
 * These averages are then persisted as normal-distributions in the summary
 * tables (one per enum every micro-batch). The processor then clusters these
 * averages to produce clusters of enumerations with similar averages. These
 * clusters are then persisted as one normal-distribution per cluster (one per
 * cluster every micro-batch).
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class CustomerSaleStream implements Serializable, SparkProcessor<String, CustomerSale> {

	private static Logger logger = LoggerFactory.getLogger(CustomerSaleStream.class);

	private static final int MIN_QUANTITY = 60_000_000;

	// write results to 'spark-results' sub-directory of user's home directory
	private static final String OUTPUT_DIRECTORY = System.getProperty("user.home") + "/spark-results/";

	// full-path to wanted output file
	private static final String OUTPUT_FILEPATH = OUTPUT_DIRECTORY + "customers.txt";

	public CustomerSaleStream() {

		logger.info("Customer Sales results will be written to '{}'.", OUTPUT_FILEPATH);

		// create results directory
		FileUtilities.createDirectory(OUTPUT_DIRECTORY);

		// delete output files before starting
		FileUtilities.deleteIfExists(OUTPUT_FILEPATH);
	}

	@Override
	public void process(final JavaPairInputDStream<String, CustomerSale> stream) {

		// extract the customer sale from the stream
		JavaDStream<CustomerSale> customers = stream.map(custStream -> custStream._2);

		// Map sales to [customer name, quantity] pairs.
		JavaPairDStream<String, Integer> allCustomerQuantities = customers.mapToPair(customer -> {
			String customerName = customer.getCustomerName();
			Integer quantity = customer.getQuantity();
			return new Tuple2<String, Integer>(customerName, quantity);
		});

		// For each customer, sum overall quantities bought
		JavaPairDStream<String, Integer> quantitiesPerCustomer = allCustomerQuantities
				.reduceByKey(new QuantitySummer());

		quantitiesPerCustomer.print();

		// Every 10 seconds create a stream of customer activity over the last
		// 60 seconds.
		JavaPairDStream<String, Integer> windowedQuantitiesPerCustomer = allCustomerQuantities
				.reduceByKeyAndWindow(new QuantitySummer(), Durations.seconds(60), Durations.seconds(10));

		// filter high volume customers over the last 60 seconds
		JavaPairDStream<String, Integer> highVolumeCustomers = windowedQuantitiesPerCustomer
				.filter(customer -> (customer._2 >= MIN_QUANTITY));

		// store high volume customers
		highVolumeCustomers.foreachRDD(new StoreCustomers());
	}

	private class QuantitySummer implements Function2<Integer, Integer, Integer> {
		@Override
		public Integer call(Integer quantity1, Integer quantity2) {
			// sum two quantities for same customer
			return quantity1 + quantity2;
		}
	}

	private class StoreCustomers implements Function2<JavaPairRDD<String, Integer>, Time, Void> {
		@Override
		public Void call(JavaPairRDD<String, Integer> customerStream, Time streamTime) throws Exception {

			// time-stamp of stream
			final Timestamp timeStamp = new Timestamp(streamTime.milliseconds());

			// map tuple and timestramp to a string
			JavaRDD<String> strings = customerStream.map(
					tuple -> "Time: '" + timeStamp + "' : Name: '" + tuple._1 + "' : Quantity '" + tuple._2 + "'.");

			/*
			 * Append all unique users to the text file.
			 */
			List<String> text = strings.collect();
			FileUtilities.appendText(OUTPUT_FILEPATH, text);
			return null;
		}
	}
}
