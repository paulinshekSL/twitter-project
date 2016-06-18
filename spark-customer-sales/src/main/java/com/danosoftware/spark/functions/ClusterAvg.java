package com.danosoftware.spark.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Class used to hold averages per cluster.
 * 
 * @author Danny
 */
public class ClusterAvg implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final List<String> clusters;
    private AvgCount average;

    public ClusterAvg(String clusterName, AvgCount average)
    {
        this.clusters = new ArrayList<>();

        this.clusters.add(clusterName);
        this.average = average;
    }

    /**
     * Adds a new value to the accumulator
     * 
     * @param x
     */
    public void add(String clusterName, AvgCount otherAverage)
    {
        clusters.add(clusterName);
        average.combine(otherAverage);
    }

    /**
     * Combines supplied accumulator into this accumulator
     * 
     * @param other
     */
    public void combine(ClusterAvg other)
    {
        clusters.addAll(other.clusters);
        average.combine(other.average);
    }

    /**
     * Calculates the average value of this accumulator
     * 
     * @return
     */
    public double avg()
    {
        return average.avg();
    }

    /**
     * Calculates the average value of this accumulator
     * 
     * @return
     */
    public List<String> clusters()
    {
        return clusters;
    }
}
