package com.danosoftware.spark.processors;

import org.apache.spark.streaming.api.java.JavaPairInputDStream;

/**
 * Spark process used to set-up the processing of Spark functions.
 * 
 * @author Danny
 *
 * @param <K>
 * @param <V>
 */
public interface SparkProcessor<K, V>
{

    /**
     * Sets-up the processing of the supplied Pair Input DStream. The
     * processing involves chaining one or more spark functions.
     * 
     * @param stream
     */
    public void process(JavaPairInputDStream<K, V> stream);

}