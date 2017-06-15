package com.danosoftware.messaging.dto;

import java.io.Serializable;

/**
 * Represents a single customer sale
 */
public class CustomerSale implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final String customerName;
    private final Integer quantity;
    private final Double value;

    public CustomerSale(String customerName, Integer quantity, Double value)
    {
        this.customerName = customerName;
        this.quantity = quantity;
        this.value = value;
    }

    public String getCustomerName()
    {
        return customerName;
    }

    public Integer getQuantity()
    {
        return quantity;
    }

    public Double getValue()
    {
        return value;
    }
}
