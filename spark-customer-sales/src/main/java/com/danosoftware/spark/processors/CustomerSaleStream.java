package com.danosoftware.spark.processors;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.beans.CustomerSale;

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
public class CustomerSaleStream implements Serializable, SparkProcessor<String, CustomerSale> {
	private static final long serialVersionUID = 1L;

	private static final int MIN_QUANTITY = 60_000_000;

	private static final String DB_USERNAME = "root";
	private static final String DB_PASSWORD = "seawolf";
	private static final String DB_URL = "jdbc:mysql://localhost:3306/customers";

	// private final DataSource dataSource;

	private static final Logger logger = LoggerFactory.getLogger(CustomerSaleStream.class);

	public CustomerSaleStream() {
		// this.dataSource = getDataSource();
	}

	@Override
	public void process(final JavaPairInputDStream<String, CustomerSale> stream) {

		// map the stream to purely customer sales (i.e. remove the string keys)
		JavaDStream<CustomerSale> customers = stream.map(new CustomerSaleExtractor());

		// customers.count().print();

		// Map sales to [customer name, quantity] pairs.
		JavaPairDStream<String, Integer> allCustomerQuantities = customers.mapToPair(new CustomerQuantitiesExtractor());

		// allCustomerQuantities.print();

		// For each customer, sum overall quantities bought
		JavaPairDStream<String, Integer> quantitiesPerCustomer = allCustomerQuantities
				.reduceByKey(new QuantitySummer());

		quantitiesPerCustomer.print();

		// Every 10 seconds create a stream of customer activity over the last
		// 60 seconds.
		JavaPairDStream<String, Integer> windowedQuantitiesPerCustomer = allCustomerQuantities
				.reduceByKeyAndWindow(new QuantitySummer(), Durations.seconds(60), Durations.seconds(10));

		// windowedQuantitiesPerCustomer.print();

		// filter high volume customers over the last 60 seconds
		JavaPairDStream<String, Integer> highVolumeCustomers = windowedQuantitiesPerCustomer
				.filter(new HighQuantityFilter());

		// highVolumeCustomers.print();

		// store high volume customers
		highVolumeCustomers.foreachRDD(new StoreCustomers());
	}

	/**
	 * return an instance of the wanted mySQL datasource
	 */
	// private DataSource getDataSource()
	// {
	// MysqlConnectionPoolDataSource mysqlDataSource = new
	// MysqlConnectionPoolDataSource();
	// mysqlDataSource.setUser(DB_USERNAME);
	// mysqlDataSource.setPassword(DB_PASSWORD);
	// mysqlDataSource.setUrl(DB_URL);
	//
	// return mysqlDataSource;
	// }

	@SuppressWarnings("serial")
	private class CustomerSaleExtractor implements Function<Tuple2<String, CustomerSale>, CustomerSale> {
		@Override
		public CustomerSale call(Tuple2<String, CustomerSale> tuple) throws Exception {
			// return the Customer Sale from the supplied tuple
			return tuple._2();
		}
	}

	@SuppressWarnings("serial")
	private class CustomerQuantitiesExtractor implements PairFunction<CustomerSale, String, Integer> {
		@Override
		public Tuple2<String, Integer> call(CustomerSale sale) throws Exception {
			/*
			 * create a tuple of customer name to quantity sold
			 */
			String customerName = sale.getCustomerName();
			Integer quantity = sale.getQuantity();

			return new Tuple2<String, Integer>(customerName, quantity);
		}
	}

	@SuppressWarnings("serial")
	private class QuantitySummer implements Function2<Integer, Integer, Integer> {
		@Override
		public Integer call(Integer quantity1, Integer quantity2) {
			// sum two quantities for same customer
			return quantity1 + quantity2;
		}
	}

	@SuppressWarnings("serial")
	private class HighQuantityFilter implements Function<Tuple2<String, Integer>, Boolean> {
		@Override
		public Boolean call(Tuple2<String, Integer> customerQuantities) throws Exception {
			// only pass filter if quantity greater than required minimum
			// quantity
			return customerQuantities._2 >= MIN_QUANTITY;
		}
	};

	@SuppressWarnings("serial")
	private class StoreCustomers implements Function2<JavaPairRDD<String, Integer>, Time, Void> {
		@Override
		public Void call(JavaPairRDD<String, Integer> customerStream, Time streamTime) throws Exception {
			// time-stamp of stream
			final Timestamp timeStamp = new Timestamp(streamTime.milliseconds());

			List<Tuple2<String, Integer>> customers = customerStream.collect();
			for (Tuple2<String, Integer> aCustomer : customers) {
				// insert into database
				// databaseInsert(timeStamp, aCustomer._1, aCustomer._2);
			}

			return null;
		}

		// format stream time-stamp
		// SimpleDateFormat directoryFormat = new
		// SimpleDateFormat("yyyyMMddhhmmss");
		// String directoryTimeStamp = directoryFormat.format(new
		// Timestamp(v2.milliseconds()));
		// SimpleDateFormat fileFormat = new SimpleDateFormat("dd/MM/yyyy
		// hh:mm:ss");
		// final String fileTimeStamp = fileFormat.format(new
		// Timestamp(v2.milliseconds()));

		// format stream time-stamp
		// SimpleDateFormat directoryFormat = new
		// SimpleDateFormat("yyyyMMddhhmmss");
		// String directoryTimeStamp = directoryFormat.format(new
		// Timestamp(v2.milliseconds()));
		// SimpleDateFormat fileFormat = new SimpleDateFormat("dd/MM/yyyy
		// hh:mm:ss");
		// final String fileTimeStamp = fileFormat.format(new
		// Timestamp(v2.milliseconds()));

		// // create RDD of lines in CSV file
		// JavaRDD<String> strings = v1.coalesce(1, true).mapPartitions(new
		// FlatMapFunction<Iterator<Tuple2<String, Integer>>, String>()
		// {
		// /*
		// * TODO : Function needs re-writing as no longer needs to return
		// stings.
		// * We are no longer writing strings to a text. Should return a
		// collection
		// * of objects to insert into a database in batch-mode.
		// */
		// @Override
		// public Iterable<String> call(Iterator<Tuple2<String, Integer>>
		// customers) throws Exception
		// {
		// List<String> text = new ArrayList<>();
		//
		// /*
		// * Insert each row into database
		// */
		// while (customers.hasNext())
		// {
		// Tuple2<String, Integer> aCustomer = customers.next();
		// text.add(fileTimeStamp + "," + aCustomer._1() + "," + aCustomer._2);
		// logger.debug("Saving Customer: '{}'. Value: '{}'.", aCustomer._1,
		// aCustomer._2);
		// }
		//
		// return text;
		// }
		// });

		/*
		 * Insert high value customers into database
		 */
		// private void databaseInsert(Timestamp timeStamp, String customer,
		// Integer quantity) {
		// try (Connection conn = dataSource.getConnection()) {
		// PreparedStatement pstmt = conn.prepareStatement(
		// "INSERT INTO high_value_customers (timestamp,customer_name,quantity)
		// VALUES(?,?,?);");
		// pstmt.setTimestamp(1, timeStamp);
		// pstmt.setString(2, customer);
		// pstmt.setInt(3, quantity);
		// pstmt.execute();
		// pstmt.close();
		// } catch (SQLException e) {
		// logger.error("SQL Exception");
		// }
		// }
	}
}
