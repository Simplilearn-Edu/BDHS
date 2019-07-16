package com.com.simplilearn.bigdata.spark

import java.util

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SparkSession, functions}

object SparkLauncher {

  def main(arg: Array[String]) {

    if (arg.length != 2) {
      System.out.println("Please provide <input_path> <spark_master>")
      System.exit(0)
    }
    val inputPath: String = arg(0)
    val dataset = readFile(inputPath, readWithHeader(getSparkSession("spark-retail-transaction-analysis", arg(1))))

    topFiveBrandsSoldMost(dataset)
    topFiveProductsSoldMost(dataset)
    productCountAndRevenueQuarterly(dataset)
    productCountQuarterlyByCategory(dataset)
    productCountQuarterlyByBrand(dataset)
    topFiveProfitableProductsSold(dataset)
    topFiveProfitableBrandCategorywise(dataset)
    bottomFiveBrandByOrderCountAndCategorywise(dataset)
    topFiveProfitableProductsSoldMonthly(dataset)
    bottomFiveLeastSellingProductsSoldMonthly(dataset)
  }

  /**
    * Solution
    *  5.a.i.1
    *
    * @param dataset
    */
  def topFiveBrandsSoldMost(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.withColumn("month", UDFUtils.toMonth(dataset("InvoiceDate"))).drop("InvoiceDate")
    System.out.println("Dataset Schema " + modifiedDataset.schema)
    modifiedDataset = modifiedDataset.filter("SubCategory='mobile'").filter("month > 9 OR month = 0")
    System.out.println("Row Count" + modifiedDataset.count())
    modifiedDataset = modifiedDataset.select("month", "brand", "Quantity").groupBy("month", "brand").agg(functions.sum("Quantity").as("OrderCount"))
    val months = util.Arrays.asList(0, 10, 11)
    var i = 0
    for (i <- 0 to months.size() - 1) {
      val topBrandsSold = modifiedDataset.filter("month = " + months.get(i)).sort(functions.desc("OrderCount")).limit(5).withColumn("MonthName", UDFUtils.toMonthName(modifiedDataset("Month"))).drop("Month")
      print("Top 5 Brands having maximum Order in mobile category ", topBrandsSold.collectAsList.toArray, topBrandsSold.columns.toArray)
    }
  }




  /**
    * Solution
    *  5.a.i.2
    *
    * @param dataset
    */
  private def topFiveProductsSoldMost(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.withColumn("month", UDFUtils.toMonth(dataset("InvoiceDate"))).drop("InvoiceDate")
    System.out.println("Dataset Schema " + modifiedDataset.schema)
    modifiedDataset = modifiedDataset.filter("month > 9 OR month = 0")
    System.out.println("Row Count" + modifiedDataset.count())
    modifiedDataset = modifiedDataset.select("month", "ProductCode", "Description", "Quantity").groupBy("month", "ProductCode", "Description").agg(functions.sum("Quantity").as("OrderCount"))
    val months = util.Arrays.asList(0, 10, 11)
    var i = 0
    for (i <- 0 to months.size() - 1) {
      val topProductsSold = modifiedDataset.filter("month = " + months.get(i)).sort(functions.desc("OrderCount")).limit(5).withColumn("MonthName", UDFUtils.toMonthName(modifiedDataset("Month"))).drop("Month")
      print("Top 5 products having maximum Order in mobile category ", topProductsSold.collectAsList.toArray, topProductsSold.columns.toArray)
    }
  }

  /**
    * Solution
    *  5.a.ii.1
    *
    * @param dataset
    */
  private def productCountAndRevenueQuarterly(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.withColumn("Quarter", UDFUtils.toQuarter(dataset("InvoiceDate"))).drop("InvoiceDate")
    System.out.println("Dataset Schema " + modifiedDataset.schema)
    modifiedDataset = modifiedDataset.select("Quarter", "TransactionAmount", "Quantity").groupBy("Quarter").agg(functions.sum("Quantity").as("Inventory Sold"), functions.sum("TransactionAmount").as("Total Revenue"))
    val productStatsQuaterly = modifiedDataset.withColumn("Quarter Name", UDFUtils.toQuarterName(modifiedDataset("Quarter"))).withColumn("Total Revenue", UDFUtils.doubleToString(modifiedDataset("Total Revenue"))).drop("Quarter").sort(functions.asc("Quarter"))
    print("Quarterly revenue and sold inventory ", productStatsQuaterly.collectAsList.toArray, productStatsQuaterly.columns.toArray)
  }

  /**
    * Solution
    *  5.a.ii.2
    *
    * @param dataset
    */
  private def productCountQuarterlyByCategory(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.withColumn("Quarter", UDFUtils.toQuarter(dataset("InvoiceDate"))).drop("InvoiceDate")
    System.out.println("Dataset Schema " + modifiedDataset.schema)
    modifiedDataset = modifiedDataset.select("Quarter", "Category", "Quantity").groupBy("Quarter", "Category").agg(functions.sum("Quantity").as("Inventory Sold"))
    val productStatsByCategoryQuaterly = modifiedDataset.withColumn("Quarter Name", UDFUtils.toQuarterName(modifiedDataset("Quarter"))).drop("Quarter").sort(functions.asc("Quarter"))
    print("Quarterly sold inventory for each Category", productStatsByCategoryQuaterly.collectAsList.toArray, productStatsByCategoryQuaterly.columns.toArray)
  }

  /**
    * Solution
    *  5.a.ii.3
    *
    * @param dataset
    */
  private def productCountQuarterlyByBrand(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.withColumn("Quarter", UDFUtils.toQuarter(dataset("InvoiceDate"))).drop("InvoiceDate")
    System.out.println("Dataset Schema " + modifiedDataset.schema)
    modifiedDataset = modifiedDataset.select("Quarter", "Brand", "Quantity").groupBy("Quarter", "Brand").agg(functions.sum("Quantity").as("Inventory Sold"))
    val productStatsByBrandQuaterly = modifiedDataset.withColumn("Quarter Name", UDFUtils.toQuarterName(modifiedDataset("Quarter"))).drop("Quarter").sort(functions.asc("Quarter"))
    print("Quarterly sold inventory for each Brand", productStatsByBrandQuaterly.collectAsList.toArray, productStatsByBrandQuaterly.columns.toArray)
  }

  /**
    * Solution
    *  5.a.iii.1
    *
    * @param dataset
    */
  private def topFiveProfitableProductsSold(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.select("ProductCode", "Description", "Marginpercentage", "TransactionAmount").withColumn("Profit", UDFUtils.calculateProfit(dataset("Marginpercentage"),dataset("TransactionAmount"))).drop("Marginpercentage", "TransactionAmount").groupBy("ProductCode", "Description").agg(functions.sum("Profit").as("Profit")).sort(functions.desc("Profit")).limit(5)
    modifiedDataset = modifiedDataset.withColumn("Profit", UDFUtils.doubleToString(modifiedDataset("Profit")))
    print("Top 5 products profitable products ", modifiedDataset.collectAsList.toArray, modifiedDataset.columns.toArray)
  }

  /**
    * Solution
    *  5.a.iv.1
    *
    * @param dataset
    */
  private def topFiveProfitableBrandCategorywise(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.select("Category", "SubCategory", "Brand", "Marginpercentage", "TransactionAmount").withColumn("Profit", UDFUtils.calculateProfit(dataset("Marginpercentage"),dataset("TransactionAmount"))).drop("Marginpercentage", "TransactionAmount").groupBy("Category", "SubCategory", "Brand").agg(functions.sum("Profit").as("Profit")).sort(functions.desc("Profit")).limit(5)
    modifiedDataset = modifiedDataset.withColumn("Profit", UDFUtils.doubleToString(modifiedDataset("Profit")))
    print("Top 5 profitable brands category wise ", modifiedDataset.collectAsList.toArray, modifiedDataset.columns.toArray)
  }

  /**
    * Solution
    *  5.a.iv.2
    *
    * @param dataset
    */
  private def bottomFiveBrandByOrderCountAndCategorywise(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.select("Category", "SubCategory", "Brand", "Quantity").groupBy("Category", "SubCategory", "Brand").agg(functions.sum("Quantity").as("Quantity")).sort(functions.asc("Quantity")).limit(5)
    print("Bottom 5 brands category wise which has less orders ", modifiedDataset.collectAsList.toArray, modifiedDataset.columns.toArray)
  }

  /**
    * Solution
    *  5.a.v.1
    *
    * @param dataset
    */
  private def topFiveProductByOrderCount(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.withColumn("Month", UDFUtils.toMonth(modifiedDataset("InvoiceDate"))).drop("InvoiceDate")
    modifiedDataset =    modifiedDataset.select("Month", "ProductCode", "Description", "Quantity").groupBy("Month", "ProductCode", "Description").agg(functions.sum("Quantity").as("Quantity"))
    var i = 0
    for (i <- 0 to 11) {
      val topProductsSoldMonthly = modifiedDataset.filter("month = " + i).sort(functions.desc("Quantity")).limit(5).withColumn("MonthName", UDFUtils.toMonthName(modifiedDataset("Month"))).drop("Month")
      print("Top 5 product which has maximum orders = ", topProductsSoldMonthly.collectAsList.toArray, topProductsSoldMonthly.columns.toArray)
    }
  }

  /**
    * Solution
    *  5.a.v.2
    *
    * @param dataset
    */
  private def topFiveProfitableProductsSoldMonthly(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.withColumn("month", UDFUtils.toMonth(dataset("InvoiceDate"))).drop("InvoiceDate")
    modifiedDataset = modifiedDataset.select("Month", "ProductCode", "Description", "Marginpercentage", "TransactionAmount").withColumn("Profit", UDFUtils.calculateProfit(modifiedDataset("Marginpercentage"),modifiedDataset("TransactionAmount"))).drop("Marginpercentage", "TransactionAmount").groupBy("Month", "ProductCode", "Description").agg(functions.sum("Profit").as("Profit"))
    var i = 0
    for (i <- 0 to 11) {
      val topProductsSoldMonthly = modifiedDataset.filter("month = " + i).sort(functions.desc("Profit")).withColumn("Profit", UDFUtils.doubleToString(modifiedDataset("Profit"))).limit(5).withColumn("MonthName", UDFUtils.toMonthName(modifiedDataset("Month"))).drop("Month")
      print("Top 5 products profitable products monthly ", topProductsSoldMonthly.collectAsList.toArray, topProductsSoldMonthly.columns.toArray)
    }
  }

  /**
    * Solution
    *  5.a.v.3
    *
    * @param dataset
    */
  private def bottomFiveLeastSellingProductsSoldMonthly(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.withColumn("month", UDFUtils.toMonth(dataset("InvoiceDate"))).drop("InvoiceDate")
    modifiedDataset = modifiedDataset.select("Month", "ProductCode", "Description", "Quantity").groupBy("Month", "ProductCode", "Description").agg(functions.sum("Quantity").as("Quantity"))
    var i = 0
    for (i <- 0 to 11) {
      val leastSellingProductsMonthly = modifiedDataset.filter("month = " + i).sort(functions.asc("Quantity")).limit(5).withColumn("MonthName", UDFUtils.toMonthName(modifiedDataset("Month"))).drop("Month")
      print("Bottom 5 products least selling products monthly ", leastSellingProductsMonthly.collectAsList.toArray, leastSellingProductsMonthly.columns.toArray)
    }
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
}
