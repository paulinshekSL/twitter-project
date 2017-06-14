# Real-time Customer Sales Processing using Spark Streaming
This project is a demonstration of a simple real-time data processing pipeline using Apache Kafka and Apache Spark Streaming.

In this demonstration, a continuous stream of random customer sales records are pushed into an Apache Kafka messgaing queue. These customer sales records are consumed as a stream by Apache Spark. These customer sales records are summarised in a sliding window using Apache Spark stream processing and these results are continuously appended to a text file.

**NOTE**: A future implementation plans to write these results to a suitable scalable database (such as Apache Cassandra).

### Getting Started

Start by downloading or cloning this repostory to a directory of your choice. It contains:

  - Helper bash scripts to start/stop Apache Kafka
  - Java Customer Sales Producer
  - Java Customer Sales Consumer

Start by building the projects using Maven.

```sh
$ cd <path-to-cloned-repository>/customer-sales
$ mvn clean install
```

### Installing Apache Kafka

Apache Kafka is used to hold the customer sales records published by the producer. These customer sales records will be retrieved by the consumer.

If you haven't already, you need to download Apache Kafka from:
https://kafka.apache.org/downloads

This has been tested against version `2.11-0.9.0.0` but the latest available version should be suitable. Unpack the files to a directory of your choice.

### Starting Apache Kafka

A bash script has been created that simplifies the start and stop of Apache Kafka. First set an environmental variable that the scripts use to find the Kafka scripts they need. For example:

```sh
$ export APACHE_KAFKA_DIR=/home/user1/my-apps/kafka_2.11-0.9.0.0
```

You can now run the helper script that starts up Apache Kafka and creates the customer sales topic.

```sh
$ cd <path-to-cloned-repository>/scripts
$ ./start_kafka.bash
```

**NOTE**: If you don't want to set the `APACHE_KAFKA_DIR` environmental variable every time, you could hard-code yours as a variable inside the start and stop scripts.

### Installing Apache Spark

Apache Spark is used to consume and process the customer sales records stream.

If you haven't already, you need to download Apache Spark from:
https://spark.apache.org/downloads.html

This has been tested against version `1.6.0 (Pre-built for Apache Hadoop 2.6)` but the latest available version pre-built for Hadoop should be suitable. Unpack the files to a directory of your choice.

### Starting Apache Spark

Once unpacked you need to start the Spark cluster (consisting of a single node at this stage). This can be started using the scripts provided with Spark. Navigate to the directory you unpacked Spark to and run the start script. For example:

```sh
$ cd /home/user1/my-apps/spark-1.6.0-bin-hadoop2.6
$ ./sbin/start-all.sh
```

You can check Spark is running using the Spark UI at:
http://localhost:8080/

This should show we have one worker available (our machine) but no applications running.

Make a note of the Spark Master URL. This will be needed to submit applications to the cluster later. This will be in the format: `spark://<machine-name>:<port>`. The default port is normally: `7077`.


### Submitting a Apache Spark Application

Next we will submit our customer sales consumer to the Spark cluster. Submitting this application to Spark will cause it to be run by Spark using the cluster. The application will start consuming customer sales from Kafka, processing them and outputting summary results.

The consumer application will have been built at: `<path-to-cloned-repository>/customer-sales-spark-consumer/target/customer-sales-consumer.jar`. You can leave the file here or copy it elsewhere.

To submit the application you will also need the Spark Master URL you saw earlier in the Spark UI (e.g. `my-machine-name.local:7077`). You can then submit the application using the Spark submit scripts. For example:

```sh
$ cd /home/user1/my-apps/spark-1.6.0-bin-hadoop2.6
$ ./bin/spark-submit --master spark://my-machine-name.local:7077 file:///<path-to-consumer-jar>/customer-sales-consumer.jar
```

The customer sales consumer should now be running on the Spark cluster. You can check that we now have a running Spark application called `CustomerSalesProcessor` on the Spark UI:
http://localhost:8080/

### Creating a Stream of Customer Sales

The producer application that creates a stream of customer sales will have been built at: `<path-to-cloned-repository>/customer-sales-producer/target/customer-sales-producer.jar`. You can either run this jar directly or run the main class `com.danosoftware.spark.main.CustomerSalesProducer` from an IDE.

Once the producer starts running, customer sales will be pushed into the Kafka messaging queue, which will then be consumed by the Spark application.

### Monitoring Spark Stream Processing

The spark stream processing can be monitored using the Spark application UI at:
http://localhost:4040/

From here, click on `Streaming` tab to monitor:

  - Streaming Input Rate (how many customer sales consumed per second)
  - Scheduling Delay (how long batches of customer sales are waiting to be processed)
  - Processing Time (how long batches of customer sales took to process)
  - Review performance of individual batches
  
By default, the Spark application will write high volume customers (based on the received customer sales) to a sub-directory of your home directory: `/home/user1/spark-results/customers.txt`.
  
When finished, stop the producer and watch how the streaming input rate drops to zero and the pending queue drops to zero once everything has been processed.

### Shutting Down Apache Spark

Stop the Spark application by killing it in the Spark UI or stopping the Spark submit process.

Use the built in Spark script to shutdown the Apache Spark cluster. For example:

```sh
$ cd /home/user1/my-apps/spark-1.6.0-bin-hadoop2.6
$ ./sbin/stop-all.sh 
```

### Shutting Down Apache Kafka

Use the provided bash script to stop of Apache Kafka. For example:

```sh
$ cd <path-to-cloned-repository>/scripts
$ ./stop_kafka.bash
```

This can sometimes fail to shutdown Kafka correctly. Any stray Kafka processes can also cause problems next time you attempt to start Kafka. Double-check for any Kafka processes still running:

```sh
$ ps aux | grep kafka
```

Manually kill any Kafka processes still running.
