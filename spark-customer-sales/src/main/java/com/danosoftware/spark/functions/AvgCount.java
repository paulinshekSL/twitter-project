package com.danosoftware.spark.functions;

import java.io.Serializable;

/**
 * Class used to hold sum of values seen and number of values seen to
 * allow calculation of average values.
 * 
 * @author Danny
 */
public class AvgCount implements Serializable
{
    private static final long serialVersionUID = 1L;

    private double sum;
    private int num;

    public AvgCount(double sum, int num)
    {
        this.sum = sum;
        this.num = num;
    }

    /**
     * Adds a new value to the accumulator
     * 
     * @param x
     */
    public void add(double x)
    {
        sum += x;
        num += 1;
    }

    /**
     * Combines supplied accumulator into this accumulator
     * 
     * @param other
     */
    public void combine(AvgCount other)
    {
        this.sum += other.sum;
        this.num += other.num;
    }

    /**
     * Calculates the average value of this accumulator
     * 
     * @return
     */
    public double avg()
    {
        return sum / (double) num;
    }
}
