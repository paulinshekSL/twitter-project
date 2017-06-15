package com.danosoftware.spark.processors;

import com.danosoftware.messaging.dto.CustomerSale;
import com.danosoftware.spark.utilities.FileUtilities;
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
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

/**
 * Spark processor that finds high volume customers over a sliding 60 second
 * window from a stream of customer sales. This is calculated every 10 seconds.
 *
 * These high volume customers are then appended continuously to a text file.
 *
 * The processor extracts individual customer name and quantities, combines them into total
 * quantities per customer. This is then summarised over a 60 second window and high volume
 * customers filtered for this period.
 * 
 * @author Danny
 *
 */
@SuppressWarnings("serial")
public class CustomerSaleStream implements Serializable, SparkProcessor<String, CustomerSale> {

	private static Logger logger = LoggerFactory.getLogger(CustomerSaleStream.class);

	// minimum quantity in last 60 seconds to be classified as high volume customer
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
		JavaPairDStream<String, Integer> allCustomerQuantities = customers.mapToPair(
				customer -> new Tuple2<String, Integer>(customer.getCustomerName(), customer.getQuantity())
		);

		// For a customer, reduce quantities by summing
		JavaPairDStream<String, Integer> quantitiesPerCustomer = allCustomerQuantities.reduceByKey(
				(quantity1, quantity2) -> quantity1 + quantity2
		);

		// Every 10 seconds summarise customer activity over the last 60 seconds.
		JavaPairDStream<String, Integer> windowedQuantitiesPerCustomer = allCustomerQuantities
				.reduceByKeyAndWindow(
						(quantity1, quantity2) -> quantity1 + quantity2,
						Durations.seconds(60),
						Durations.seconds(10)
				);

		// filter high volume customers (from results over the last 60 seconds)
		JavaPairDStream<String, Integer> highVolumeCustomers = windowedQuantitiesPerCustomer.filter(
				customer -> (customer._2 >= MIN_QUANTITY)
		);

		// store high volume customers
		highVolumeCustomers.foreachRDD(new StoreCustomers());
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
