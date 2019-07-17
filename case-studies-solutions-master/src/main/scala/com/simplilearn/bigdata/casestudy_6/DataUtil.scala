package com.simplilearn.bigdata.casestudy_6

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DataUtil {

  private val SEPERATOR = "-"

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.out.println("Please provide <input_path> <spark_master> <hbase_namenodes_config>")
      System.exit(0)
    }
    val inputPath = args(0)
    val namenodes = args(2)

    val dataset = readFile(inputPath, readWithHeader(getSparkSession("traffic-accidents-analysis", args(1))))

    getData(dataset, namenodes)
    getTotalData(dataset, namenodes)
  }

  def getTotalData(dataset: Dataset[Row], namenodes: String): Unit = {
    val modifiedDataset = dataset
        .select("Year", "Total")
        .groupBy("Year")
        .agg(functions.sum("Total"))

    modifiedDataset.foreach(
      row => {
        var table:Table = null
        try {
          val connection = HbaseManager.hbaseConnection(namenodes)
          val rowKey = Bytes.toBytes(row.get(0).toString)
          val get:Get = new Get(rowKey)
          table = connection.getTable(TableName.valueOf("total_traffic_accidents_yearly"))
          val result = table.get(get)
          if (result.size  == 0) {
            val put:Put = new Put(rowKey)
            put.addColumn(Bytes.toBytes("total"), Bytes.toBytes("count"), Bytes.toBytes(row.get(1).asInstanceOf[Long].toString))
            table.put(put)
          }
        } finally try
          table.close
        catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
      }
    )
  }

  def getData(dataset: Dataset[Row], namenodes: String): Unit = {
    val modifiedDataset = dataset
      .groupBy("Year", "TYPE")
      .agg(
        functions.sum("0-3_hrs"),
        functions.sum("3-6_hrs"),
        functions.sum("6-9_hrs"),
        functions.sum("9-12_hrs"),
        functions.sum("12-15_hrs"),
        functions.sum("15-18_hrs"),
        functions.sum("18-21_hrs"),
        functions.sum("21-24_hrs"),
        functions.sum("Total"))

    modifiedDataset.foreach(
      row => {
        var table:Table = null
        try {
          val connection = HbaseManager.hbaseConnection(namenodes)
          val rowKey = Bytes.toBytes(row.get(0).toString + SEPERATOR + row.get(1).toString)
          val get:Get = new Get(rowKey)
          table = connection.getTable(TableName.valueOf("traffic_accidents_yearly"))
          val result = table.get(get)
          if (result.size  == 0) {
            val put:Put = new Put(rowKey)
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("0-3"), Bytes.toBytes(row.get(2).asInstanceOf[Double].toString))
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("3-6"), Bytes.toBytes(row.get(3).asInstanceOf[Double].toString))
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("6-9"), Bytes.toBytes(row.get(4).asInstanceOf[Double].toString))
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("9-12"), Bytes.toBytes(row.get(5).asInstanceOf[Double].toString))
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("12-15"), Bytes.toBytes(row.get(6).asInstanceOf[Double].toString))
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("15-18"), Bytes.toBytes(row.get(7).asInstanceOf[Double].toString))
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("18-21"), Bytes.toBytes(row.get(8).asInstanceOf[Double].toString))
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("21-24"), Bytes.toBytes(row.get(9).asInstanceOf[Double].toString))
            put.addColumn(Bytes.toBytes("total"), Bytes.toBytes("count"), Bytes.toBytes(row.get(10).asInstanceOf[Long].toString))
            table.put(put)
          }
        } finally try
          table.close
        catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
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
      StructField("Year", IntegerType, true),
      StructField("TYPE", StringType, true),

      StructField("0-3_hrs", StringType, true),
      StructField("3-6_hrs", StringType, true),
      StructField("6-9_hrs", StringType, true),
      StructField("9-12_hrs", StringType, true),
      StructField("12-15_hrs", StringType, true),
      StructField("15-18_hrs", StringType, true),
      StructField("18-21_hrs", StringType, true),
      StructField("21-24_hrs", StringType, true),
      StructField("Total", IntegerType, true)))
    sparkSession.read.option("header", true).schema(transactionSchema).option("mode", "DROPMALFORMED")
  }
}
