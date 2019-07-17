package com.simplilearn.bigdata.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkStreamingRevenueReachedApp {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Please provide <broker_url> <topic_name> <group_id> <spark_master>");
            System.exit(0);
        }

        String brokerUrl = args[0];
        String topicName = args[1];
        String groupId = args[2];
        String sparkMaster = args[3];

        Integer durationInMinutes = 43200;
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Spark Streaming App - Revenue analytics in last "+ durationInMinutes +" minutes");
        sparkConf.setMaster(sparkMaster.indexOf("local") != -1 ? "local[*]" : sparkMaster);


        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.minutes(durationInMinutes));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokerUrl);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList(topicName);

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaPairDStream<String, String> results = messages
                .mapToPair(
                        record -> new Tuple2<>(record.key(), record.value())
                );
        JavaDStream<String> allProducts = results
                .map(
                        tuple2 -> tuple2._2()
                );


        JavaPairDStream<String, Double> totalRevenueRdd = allProducts
                .mapToPair(new PairFunction<String, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(String data) throws Exception {
                        try {
                            return new Tuple2<String, Double>("TotalRevenue", Double.parseDouble(data.split(",")[9]));
                        } catch (Exception exception) {

                        }
                        return new Tuple2<String, Double>("TotalRevenue", 0.0);
                    }
                })
                .reduceByKey((i1, i2) -> i1 + i2);

        totalRevenueRdd.foreachRDD(
                rdd -> {
                    targetStatus(rdd, durationInMinutes);
                }
        );

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private static Double OVER_ACHIEVED = 80000000.0;

    private static Double ON_TARGET = 60000000.0;

    private static void targetStatus(JavaPairRDD<String, Double> rdd, Integer durationInMinutes) {
        Double revenueAmount = rdd.collect().get(0)._2;
        if (revenueAmount >= OVER_ACHIEVED) {
            System.out.println("Target Achieved in last " + durationInMinutes + " minutes.");
        } else if (revenueAmount < OVER_ACHIEVED && revenueAmount >= ON_TARGET) {
            System.out.println("On Target in last " + durationInMinutes + " minutes.");
        } else {
            System.out.println("Target not achieved in last " + durationInMinutes + " minutes.");
        }
    }
}
