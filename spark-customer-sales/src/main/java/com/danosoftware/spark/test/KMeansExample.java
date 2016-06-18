package com.danosoftware.spark.test;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class KMeansExample
{
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("K-means Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load and parse data
        String path = "/Users/Danny/temp/kmeans_data.txt";
        JavaRDD<String> data = sc.textFile(path);

        JavaRDD<Vector> parsedData = data.map(new Function<String, Vector>()
        {
            public Vector call(String s)
            {
                String[] sarray = s.split(" ");
                double[] values = new double[sarray.length];
                for (int i = 0; i < sarray.length; i++)
                    values[i] = Double.parseDouble(sarray[i]);
                return Vectors.dense(values);
            }
        });
        parsedData.cache();

        // Cluster the data into two classes using KMeans
        int numClusters = 4;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        System.out.println(clusters.formatVersion());

        for (Vector vec : clusters.clusterCenters())
        {
            System.out.println(vec.toString());
        }

        double[] tests =
        { 28D };
        System.out.println(clusters.predict(Vectors.dense(tests)));

        JavaRDD<Integer> data22 = clusters.predict(parsedData);
        List<Integer> ints = data22.collect();
        System.out.println(ints);

        //System.out.println(clusters.clusterCenters().toString());

        // Save and load model
        //clusters.save(sc.sc(), "myModelPath");
        //KMeansModel sameModel = KMeansModel.load(sc.sc(), "myModelPath");
    }
}
