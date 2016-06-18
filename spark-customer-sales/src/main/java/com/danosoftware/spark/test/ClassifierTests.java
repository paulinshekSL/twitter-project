package com.danosoftware.spark.test;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

public class ClassifierTests
{

    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> spam = sc.textFile("/Users/Danny/temp/spam.txt");
        JavaRDD<String> normal = sc.textFile("/Users/Danny/temp/normal.txt");
        // Create a HashingTF instance to map email text to vectors of 10,000 features.
        final HashingTF tf = new HashingTF(10000);
        // Create LabeledPoint datasets for positive (spam) and negative (normal) examples.
        JavaRDD<LabeledPoint> posExamples = spam.map(new Function<String, LabeledPoint>()
        {
            public LabeledPoint call(String email)
            {
                return new LabeledPoint(1, tf.transform(Arrays.asList(email.split(" "))));
            }
        });
        JavaRDD<LabeledPoint> negExamples = normal.map(new Function<String, LabeledPoint>()
        {
            public LabeledPoint call(String email)
            {
                return new LabeledPoint(0, tf.transform(Arrays.asList(email.split(" "))));
            }
        });

        System.out.println(posExamples.collect().toString());
        for (LabeledPoint point : posExamples.collect())
        {
            System.out.println(point.toString());

            Vector vec = point.features();
            System.out.println("Vector:" + vec.toString());
            System.out.println("Label:" + point.label());

        }

        JavaRDD<LabeledPoint> trainData = posExamples.union(negExamples);
        trainData.cache(); // Cache since Logistic Regression is an iterative algorithm.
        // Run Logistic Regression using the SGD algorithm.
        LogisticRegressionModel model = new LogisticRegressionWithSGD().run(trainData.rdd());
        // Test on a positive example (spam) and a negative one (normal).
        Vector posTest = tf.transform(Arrays.asList("O M G GET cheap stuff by sending money to ...".split(" ")));
        Vector negTest = tf.transform(Arrays.asList("Hi Dad, I started studying Spark the other ...".split(" ")));
        System.out.println("Prediction for positive example: " + model.predict(posTest));
        System.out.println("Prediction for negative example: " + model.predict(negTest));
    }
}
