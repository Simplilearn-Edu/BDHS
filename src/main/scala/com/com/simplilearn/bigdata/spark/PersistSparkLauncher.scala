package com.com.simplilearn.bigdata.spark

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql._
import org.apache.spark.sql.types._


object PersistSparkLauncher {

  private val SEPERATOR = "-"

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.out.println("Please provide <input_path> <spark_master> <namenode_config>")
      System.exit(0)
    }
    val inputPath = args(0)
    val namenodes = args(2)
    val dataset = readFile(inputPath, readWithHeader(getSparkSession("spark-retail-transaction-analysis-hbase-monthly", args(1))))
    persistOrderCountByBrandMonthly(dataset, namenodes)
    persistRevenueByBrandMonthly(dataset, namenodes)
    persistOrderCountByCategoryMonthly(dataset, namenodes)
    persistRevenueByCategoryMonthly(dataset, namenodes)
  }

  def getSparkSession(appName: String, master: String) = {
    val sparkSession = SparkSession.builder.appName(appName).master(if (master.equalsIgnoreCase("local")) "local[*]"
    else master).getOrCreate
    System.out.println("Spark version " + sparkSession.version)
    sparkSession
  }

  private def getSparkContezxt(appName: String, master: String) = getSparkSession(appName, master).sparkContext

  def readWithHeader(sparkSession: SparkSession) = {
    val transactionSchema = StructType(Array(
      StructField("ProductCode", StringType, true),
      StructField("Description", StringType, true),
      StructField("Brand", StringType, true),
      StructField("Category", StringType, true),
      StructField("SubCategory", StringType, true),
      StructField("ActualPrice", DoubleType, true),
      StructField("Marginpercentage", IntegerType, true),
      StructField("Discountpercentage", IntegerType, true),
      StructField("Quantity", IntegerType, true),
      StructField("TransactionAmount", DoubleType, true),
      StructField("InvoiceDate", TimestampType, true),
      StructField("Mode", StringType, true),
      StructField("InvoiceNumber", IntegerType, true),
      StructField("CustomerId", StringType, true),
      StructField("Country", StringType, true)));
    sparkSession.read.option("header", true).schema(transactionSchema).option("mode", "DROPMALFORMED")
  }

  def print(key: String, values: Array[AnyRef], colNames: Array[AnyRef]): Unit = {
    val logBuilder = new StringBuilder
    logBuilder.append(key + " = \n")
    var i = 0
    for (i <- 0 to colNames.length - 1) {
      logBuilder.append(colNames(i)).append(", ");
    }
    logBuilder.append("\n")

    for (i <- 0 to values.length - 1) {
      logBuilder.append(values(i)).append(", \n");
    }
    logBuilder.append("\n")
    System.out.println(logBuilder.toString);
  }


  def readFile(path: String, dataFrameReader: DataFrameReader) = {
    System.out.println("Reading file " + path)
    val dataset = dataFrameReader.csv(path)
    System.out.println("Dataset Schema " + dataset.schema)
    System.out.println("Row Count" + dataset.count())
    dataset
  }


  /**
    * @param dataset
    */
  private def persistOrderCountByBrandMonthly(dataset: Dataset[Row], namenodes: String): Unit = {
    var modifiedDataset = dataset.withColumn("Month", UDFUtils.toMonth(dataset("InvoiceDate"))).withColumn("Year", UDFUtils.toYear(dataset("InvoiceDate")))
    modifiedDataset = modifiedDataset.select("Year", "Month", "Brand", "Quantity").groupBy("Year", "Month", "Brand").agg(functions.sum("Quantity").as("Quantity"))
    var i = 0
    for (i <- 0 to 11) {
      val orderCountByBrandMonthly = modifiedDataset.filter("month = " + i).withColumn("MonthName", UDFUtils.toMonthName(modifiedDataset("Month"))).drop("Month")
      System.out.println("Row Count" + modifiedDataset.count())
      orderCountByBrandMonthly.foreach((row: Row) => {
        var table:Table = null
        try {
          val connection = HbaseManager.hbaseConnection(namenodes)
          val rowKey = Bytes.toBytes(row.get(0).toString + SEPERATOR + row.get(3).toString + SEPERATOR + row.get(1).toString)
          val get:Get = new Get(rowKey)
          table = connection.getTable(TableName.valueOf("brand_order_count_monthly"))
          val result = table.get(get)
          if (result.size  == 0) {
            val put:Put = new Put(rowKey)
            put.addColumn(Bytes.toBytes("brand_stats"), Bytes.toBytes("data"), Bytes.toBytes(row.get(2).asInstanceOf[Long]))
            table.put(put)
          }
        } finally try
          table.close
        catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
      })
    }
  }

  private def persistRevenueByBrandMonthly(dataset: Dataset[Row], namenodes: String): Unit = {
    var modifiedDataset = dataset.withColumn("Month", UDFUtils.toMonth(dataset("InvoiceDate"))).withColumn("Year", UDFUtils.toYear(dataset("InvoiceDate")))
    modifiedDataset = modifiedDataset.select("Year", "Month", "Brand", "TransactionAmount").groupBy("Year", "Month", "Brand").agg(functions.sum("TransactionAmount").as("TransactionAmount"))
    var i = 0
    for (i <- 0 to 11) {
      val revenueByBrandMonthly = modifiedDataset.filter("month = " + i).withColumn("MonthName", UDFUtils.toMonthName(modifiedDataset("Month"))).drop("Month")
      System.out.println("Row Count" + modifiedDataset.count())
      revenueByBrandMonthly.foreach((row: Row) => {
        var table:Table = null
        try {
          val connection = HbaseManager.hbaseConnection(namenodes)
          val rowKey = Bytes.toBytes(row.get(0).toString + SEPERATOR + row.get(3).toString + SEPERATOR + row.get(1).toString)
          val get:Get = new Get(rowKey)
          table = connection.getTable(TableName.valueOf("brand_total_revenue_monthly"))
          val result = table.get(get)
          if (result.size  == 0) {
            val put:Put = new Put(rowKey)
            put.addColumn(Bytes.toBytes("brand_stats"), Bytes.toBytes("data"), Bytes.toBytes(row.get(2).asInstanceOf[Double]))
            table.put(put)
          }
        } finally try
          table.close
        catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
      })
    }
  }

  private def persistOrderCountByCategoryMonthly(dataset: Dataset[Row], namenodes: String): Unit = {
    var modifiedDataset = dataset.withColumn("Month", UDFUtils.toMonth(dataset("InvoiceDate"))).withColumn("Year", UDFUtils.toYear(dataset("InvoiceDate")))
    modifiedDataset = modifiedDataset.select("Year", "Month", "Category", "Quantity").groupBy("Year", "Month", "Category").agg(functions.sum("Quantity").as("Quantity"))
    var i = 0
    for (i <- 0 to 11) {
      val orderCountByCategoryMonthly = modifiedDataset.filter("month = " + i).withColumn("MonthName", UDFUtils.toMonthName(modifiedDataset("Month"))).drop("Month")
      System.out.println("Row Count" + modifiedDataset.count())
      orderCountByCategoryMonthly.foreach((row: Row) => {
        var table:Table = null
        try {
          val connection = HbaseManager.hbaseConnection(namenodes)
          val rowKey = Bytes.toBytes(row.get(0).toString + SEPERATOR + row.get(3).toString + SEPERATOR + row.get(1).toString)
          val get:Get = new Get(rowKey)
          table = connection.getTable(TableName.valueOf("category_order_count_monthly"))
          val result = table.get(get)
          if (result.size  == 0) {
            val put:Put = new Put(rowKey)
            put.addColumn(Bytes.toBytes("category_stats"), Bytes.toBytes("data"), Bytes.toBytes(row.get(2).asInstanceOf[Long]))
            table.put(put)
          }
        } finally try
          table.close
        catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
      })
    }
  }

  /**
    * @param dataset
    * @param namenodes
    */
  private def persistRevenueByCategoryMonthly(dataset: Dataset[Row], namenodes: String): Unit = {
    var modifiedDataset = dataset.withColumn("Month", UDFUtils.toMonth(dataset("InvoiceDate"))).withColumn("Year", UDFUtils.toYear(dataset("InvoiceDate")))
    modifiedDataset = modifiedDataset.select("Year", "Month", "Category", "TransactionAmount").groupBy("Year", "Month", "Category").agg(functions.sum("TransactionAmount").as("TransactionAmount"))
    var i = 0
    for (i <- 0 to 11) {
      val revenueByCategoryMonthly = modifiedDataset.filter("month = " + i).withColumn("MonthName", UDFUtils.toMonthName(modifiedDataset("Month"))).drop("Month")
      System.out.println("Row Count" + modifiedDataset.count())
      revenueByCategoryMonthly.foreach((row: Row) => {
        var table:Table = null
        try {
          val connection = HbaseManager.hbaseConnection(namenodes)
          val rowKey = Bytes.toBytes(row.get(0).toString + SEPERATOR + row.get(3).toString + SEPERATOR + row.get(1).toString)
          val get:Get = new Get(rowKey)
          table = connection.getTable(TableName.valueOf("category_total_revenue_monthly"))
          val result = table.get(get)
          if (result.size  == 0) {
            val put:Put = new Put(rowKey)
            put.addColumn(Bytes.toBytes("category_stats"), Bytes.toBytes("data"), Bytes.toBytes(row.get(2).asInstanceOf[Double]))
            table.put(put)
          }
        } finally try
          table.close
        catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
      })
    }
  }
}
