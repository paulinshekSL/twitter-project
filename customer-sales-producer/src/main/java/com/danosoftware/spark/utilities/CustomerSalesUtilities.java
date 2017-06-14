package com.danosoftware.spark.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.danosoftware.messaging.dto.CustomerSale;

public class CustomerSalesUtilities {
	// number of messages
	private static final int NUMBER_MESSAGES = 500000;

	// random number generator
	private static final Random rand = new Random();

	// possible enumerations - repeated enumerations make them more likely
	private static final String CUSTOMERS[] = { "Joe Bloggs", "Fred Smith", "Ola Nordmann", "Walter Plinge",
			"Ola Nordmann", "Joe Bloggs", "Joe Bloggs", "Fred Smith" };

	public static List<CustomerSale> generateCustomerSales() {
		List<CustomerSale> sales = new ArrayList<>();

		for (int i = 0; i < NUMBER_MESSAGES; i++) {
			CustomerSale sale = new CustomerSale(nextCustomer(), nextQuantity(), nextValue());
			sales.add(sale);
		}

		return sales;
	}

	/**
	 * Return next customer string
	 */
	private static String nextCustomer() {
		// generate a random enumeration
		int possibleEnumerations = CUSTOMERS.length;
		int enumerationIndex = rand.nextInt(possibleEnumerations - 1);
		return CUSTOMERS[enumerationIndex];
	}

	/**
	 * Return next value
	 */
	private static Double nextValue() {
		// generate a random value from 0 to 100
		return rand.nextDouble() * 100D;
	}

	/**
	 * Return next quantity
	 */
	private static Integer nextQuantity() {
		// generate a random value from 0 to 500
		return rand.nextInt(500);
	}
}
