package com.simplilearn.bigdata.casestudy_6

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Solution_1 {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.out.println("Please provide <hbase_namenodes_config>")
      System.exit(0)
    }
    val namenodes = args(0)
    getData("2014","Road Accidents","12-15",namenodes)
  }

  def getData(year: String, accident_type: String, time_duration: String, namenodes: String): Unit = {
    val connection = HbaseManager.hbaseConnection(namenodes)

    val rowKey = Bytes.toBytes(year+"-"+accident_type)
    val get:Get = new Get(rowKey)
    val table = connection.getTable(TableName.valueOf("traffic_accidents_yearly"))
    val result = table.get(get)
    if (result.size != 0) {
      val data = result.getValue(Bytes.toBytes("time"), Bytes.toBytes(time_duration));
      System.out.println("Result = "+Bytes.toString(data))
    }
  }
}
