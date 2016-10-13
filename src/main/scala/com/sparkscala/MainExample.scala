package com.sparkscala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object MainExample {

  def main(arg: Array[String]) {
    print("lolll-ll") 
    val jobName = "MainExample"
    val conf = new SparkConf().setAppName(jobName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/home/raghunandangupta/inputfiles/TravelData.txt")
    val split = textFile.map(lines => lines.split('\t')).map(x => (x(2), 1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).take(20)
    split.foreach(println)
  }
}
