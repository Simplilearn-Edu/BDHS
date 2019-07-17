package com.simplilearn.bigdata.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
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

public class SparkStreamingTrendingBrandMonthlyByRevenueApp {

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.out.println("Please provide <broker_url> <topic_name> <group_id>  <spark_master>  <duration_in_minutes_streaming_window> <Top X values>");
            System.exit(0);
        }

        String brokerUrl = args[0];
        String topicName = args[1];
        String groupId = args[2];
        String sparkMaster = args[3];
        Integer durationInMinutes = Integer.parseInt(args[4]);
        Integer N = Integer.parseInt(args[5]);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Spark Streaming App - Top "+N+" brand by Revenue analytics in last " + durationInMinutes + " minutes");
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

        JavaPairDStream<String, Double> trendingBrands = allProducts
                .mapToPair(new PairFunction<String, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(String data) throws Exception {
                        try {
                            return new Tuple2<String, Double>(data.split(",")[2], Double.parseDouble(data.split(",")[9]));
                        } catch (Exception exception) {

                        }
                        return new Tuple2<String, Double>(data.split(",")[2], 0.0);
                    }
                })
                .reduceByKey((i1, i2) -> i1 + i2);

        JavaPairDStream<Double, String> swappedBrandsPair = trendingBrands.mapToPair(x -> x.swap());
        JavaPairDStream<Double, String> sortedStream = swappedBrandsPair.transformToPair(s -> s.sortByKey(false));

        JavaDStream<String> topBrands = sortedStream.map(tuple -> tuple._2);

        topBrands.print(N);

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
