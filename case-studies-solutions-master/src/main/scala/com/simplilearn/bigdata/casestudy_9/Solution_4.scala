package com.simplilearn.bigdata.casestudy_9

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Solution_4 {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.out.println("Please provide <input_path> <spark_master>")
      System.exit(0)
    }
    val inputPath: String = args(0)

    val dataset = readFile(inputPath, readWithHeader(getSparkSession("companies-analysis", args(1))))

    getData(dataset)
  }

  def getData(dataset: Dataset[Row]): Unit = {
    val modifiedDataset = dataset
      .select("School_Year")
      .groupBy("School_Year")
      .count()
      .sort(functions.desc("count"))
    System.out.println("Most common reason")
    modifiedDataset.take(5).foreach(
      row => {
        System.out.println("School_Year = "+row.get(0).toString+" Count = "+row.get(1))
      }
    )
  }

  def getSparkSession(appName: String, master: String) = {
    val sparkSession = SparkSession.builder.appName(appName).master(if (master.equalsIgnoreCase("local")) "local[*]"
    else master).getOrCreate
    System.out.println("Spark version " + sparkSession.version)
    sparkSession
  }

  def readFile(path: String, dataFrameReader: DataFrameReader) = {
    System.out.println("Reading file " + path)
    val dataset = dataFrameReader.csv(path)
    System.out.println("Dataset Schema " + dataset.schema)
    System.out.println("Row Count" + dataset.count())
    dataset
  }

  def readWithHeader(sparkSession: SparkSession) = {
    val transactionSchema = StructType(Array(
      StructField("School_Year", StringType, true),
      StructField("Run_Type", StringType, true),
      StructField("Bus_No", StringType, true),
      StructField("Route_Number", StringType, true),
      StructField("Reason", StringType, true),
      StructField("Occurred_On", TimestampType, true),
      StructField("Number_Of_Students_On_The_Bus", IntegerType, true)))
    sparkSession.read.option("header", true).schema(transactionSchema).option("mode", "DROPMALFORMED")
  }
}