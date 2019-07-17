package com.simplilearn.bigdata.casestudy_13

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.hashing.MurmurHash3

object Solution_1 {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.out.println("Please provide <input_path> <spark_master>")
      System.exit(0)
    }
    val inputPath: String = args(0)

    val dataset = readFile(inputPath, readWithHeader(getSparkSession("flight-graph-analysis", args(1))))

    val originAirports = dataset.select("ORIGIN_AIRPORT").distinct();
    val destinationAirports = dataset.select("DESTINATION_AIRPORT").distinct();

    System.out.println("originAirports " + originAirports.count())
    System.out.println("destinationAirports " + destinationAirports.count())


    val edges = dataset.select("ORIGIN_AIRPORT", "DISTANCE","DESTINATION_AIRPORT").distinct().rdd.map(
      row => {
        Edge(MurmurHash3.stringHash(row.getAs(0)), MurmurHash3.stringHash(row.getAs(2)), row.getInt(1))
      }
    )
    System.out.println("edges " + edges.count())
    val graph = Graph.fromEdges(edges, defaultValue = 1);
    val routesCount = graph.edges.filter { case (Edge(org_id, dest_id, distance)) => distance > 1500 }.count()
    System.out.println("Routes having distance greatter than 1500 = "+routesCount)



    //Other solution
    val vertices: RDD[(VertexId, String)] =
      dataset.select("ORIGIN_AIRPORT").distinct().rdd.distinct().map(x => (MurmurHash3.stringHash(x.getAs(0)), x.getAs(0)))

    System.out.println("vertices " + vertices.count())

    val graphEdges = dataset.select("ORIGIN_AIRPORT", "DISTANCE","DESTINATION_AIRPORT").distinct().rdd.map(
      row => {
        Edge(MurmurHash3.stringHash(row.getAs(0)), MurmurHash3.stringHash(row.getAs(2)), row.getInt(1))
      }
    )
    System.out.println("graphEdges " + graphEdges.count())


    val distanceGraph = Graph(vertices, graphEdges);
    val numToStringVertex = distanceGraph.vertices.map(vertex => (vertex._1.toLong, vertex._2)).collectAsMap()
    val distanceRoutes =
      distanceGraph.edges
        .filter { case (Edge(org_id, dest_id, distance)) => distance > 1500 }
        .map(edge => (numToStringVertex.get(edge.srcId) , numToStringVertex.get(edge.dstId))).collect()
    System.out.println("Routes having distance greatter than 1500 = "+distanceRoutes.size)
    distanceRoutes.foreach(
      row => {
        System.out.println("Source "+row._1.get + " Destination = "+row._2.get)
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
