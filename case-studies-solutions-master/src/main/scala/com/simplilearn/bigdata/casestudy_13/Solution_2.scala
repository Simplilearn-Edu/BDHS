package com.simplilearn.bigdata.casestudy_13

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SparkSession, functions}
import scala.util.hashing.MurmurHash3

object Solution_2 {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.out.println("Please provide <input_path> <spark_master>")
      System.exit(0)
    }
    val inputPath: String = args(0)

    val dataset = readFile(inputPath, readWithHeader(getSparkSession("flight-graph-analysis", args(1))))

    val vertices: RDD[(VertexId, String)] =
      dataset.select("ORIGIN_AIRPORT").distinct().rdd.distinct().map(x => (MurmurHash3.stringHash(x.getAs(0)), x.getAs(0)))

    System.out.println("vertices " + vertices.count())

    val graphEdges =
      dataset
        .select("ORIGIN_AIRPORT", "CANCELLED", "DESTINATION_AIRPORT")
        .groupBy("ORIGIN_AIRPORT", "DESTINATION_AIRPORT")
        .agg(functions.sum("CANCELLED").as("TOTAL_CANCELLED"))
        .rdd.map(
        row => {
          Edge(MurmurHash3.stringHash(row.getAs(0)), MurmurHash3.stringHash(row.getAs(1)), row.getLong(2))
        }
      )
    System.out.println("graphEdges " + graphEdges.count())


    val distanceGraph = Graph(vertices, graphEdges);
    val flightCanceled  = distanceGraph.triplets.sortBy(_.attr, ascending = false).map(triplet =>
      triplet.attr.toString + " flights were cancelled between " + triplet.srcAttr + " and " + triplet.dstAttr + ".").take(5)

    System.out.println("Top routes where flights were cancelled max time ")
    flightCanceled.foreach(
      row => {
        System.out.println(row)
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
      StructField("AIRLINE", StringType, true),
      StructField("FLIGHT_NUMBER", StringType, true),
      StructField("ORIGIN_AIRPORT", StringType, true),
      StructField("DESTINATION_AIRPORT", StringType, true),
      StructField("DISTANCE", IntegerType, true),
      StructField("ARRIVAL_DELAY", IntegerType, true),
      StructField("DIVERTED", IntegerType, true),
      StructField("CANCELLED", IntegerType, true)))
    sparkSession.read.option("header", true).schema(transactionSchema).option("mode", "DROPMALFORMED")
  }
}
