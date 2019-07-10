package com.com.simplilearn.bigdata.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, StreamingContext}

object SparkStreamingRevenueReachedApp {

  private val OVER_ACHIEVED = 80000000.0

  private val ON_TARGET = 60000000.0

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.out.println("Please provide <broker_url> <topic_name> <group_id> <spark_master>")
      System.exit(0)
    }
    val brokerUrl = args(0)
    val topicName = args(1)
    val groupId = args(2)
    val sparkMaster = args(3)
    val durationInMinutes = 43200
//    val durationInMinutes = 1
    val sparkConf = new SparkConf
    sparkConf.setAppName("Spark Streaming App - Revenue analytics in last " + durationInMinutes + " minutes")
    sparkConf.setMaster(if (sparkMaster.indexOf("local") != -1) "local[*]"
    else sparkMaster)
    val streamingContext = new StreamingContext(sparkConf, Minutes(durationInMinutes))
    val kafkaParams = collection.mutable.Map[String, Object]()
    kafkaParams.put("bootstrap.servers", brokerUrl)
    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id", groupId)
    kafkaParams.put("auto.offset.reset", "latest")
    kafkaParams.put("enable.auto.commit", "false")
    val topics = Array(topicName)

    val kafkaStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,String](topics, kafkaParams))
    val allProducts = kafkaStream.map((record) => (record.value))
    val totalRevenueRdd = allProducts.map((record) => ("TotalRevenue", record.toString.split(",")(9).toDouble)).reduceByKey((i1, i2) => i1 + i2)

    totalRevenueRdd.foreachRDD((rdd) => {
      val revenueAmount = rdd.collect()(0)._2
      if (revenueAmount >= OVER_ACHIEVED)
        System.out.println("Target Achieved in last " + durationInMinutes + " minutes and renenue amount is "+revenueAmount)
      else if (revenueAmount < OVER_ACHIEVED && revenueAmount >= ON_TARGET)
        System.out.println("On Target in last " + durationInMinutes + " minutes and renenue amount is "+revenueAmount)
      else
        System.out.println("Target not achieved in last " + durationInMinutes + " minutes and renenue amount is "+revenueAmount)
    })

    streamingContext.start
    streamingContext.awaitTermination
  }
}
