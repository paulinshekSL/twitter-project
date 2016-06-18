package com.danosoftware.spark.old;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class SparkKafkaTest
{

    private static final Pattern SPACE = Pattern.compile(" ");

    @SuppressWarnings("serial")
    public static void main(String args[])
    {

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");

        Set<String> topicsSet = new HashSet<String>(Arrays.asList("testStringTopic"));

        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(jssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

        /*
         * Simple count of items seen in batch
         */
        JavaDStream<Long> count = kafkaStream.count();
        count.print();

        /*
         * Count the number of unique words seen
         */

        // compute lines
        JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String, String>, String>()
        {
            @Override
            public String call(Tuple2<String, String> tuple2)
            {
                return tuple2._2();
            }
        });

        // compute words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>()
        {
            @Override
            public Iterable<String> call(String x)
            {
                return Lists.newArrayList(SPACE.split(x));
            }
        });

        // count unique words
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>()
        {
            @Override
            public Tuple2<String, Integer> call(String s)
            {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>()
        {
            @Override
            public Integer call(Integer i1, Integer i2)
            {
                return i1 + i2;
            }
        });

        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

}
