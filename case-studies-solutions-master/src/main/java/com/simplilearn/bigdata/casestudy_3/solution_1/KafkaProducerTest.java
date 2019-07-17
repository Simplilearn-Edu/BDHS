package com.simplilearn.bigdata.casestudy_3.solution_1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class KafkaProducerTest {

    public static void main(String[] args) throws Exception{
        if(args.length != 3) {
            System.out.println("Please provide <broker> <topic> <csvfilepath>");
            System.exit(0);
        }
        Calendar cal2 = Calendar.getInstance();
        cal2.add(Calendar.MINUTE, 10);
        KafkaProducer<String, String> producer = getKafkaProducer(args[0]);
        List<String> allData = readAllData(args[2]);
        for(String data : allData) {
            producer.send(new ProducerRecord<>(args[1], data), (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("Exception occured = "+exception);
                }
                if(metadata != null) {
                    System.out.println("producing data to topic  = "+metadata.topic());
                }
            });
            try {
                Thread.sleep(5000);
            }catch(Exception exception) {
                System.out.println("Exception occured = "+exception);
            }
        }
    }

    private static KafkaProducer<String, String> getKafkaProducer(String bootStrapServer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        addAdditionalProps(props);
        return new KafkaProducer<String, String>(props, new StringSerializer(),
                new StringSerializer());
    }

    private static void addAdditionalProps(Properties props) {
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("max.request.size", 52000000);
        props.put("max.block.ms",10000);
        props.put("request.timeout.ms",5000);
    }

    private static List<String> readAllData(String path) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(path));
        List<String> allData = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            allData.add(line);
        }
        System.out.println("Total line count including header = "+allData.size());
        return allData;
    }
}