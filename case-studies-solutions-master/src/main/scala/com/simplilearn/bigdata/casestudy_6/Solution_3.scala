package com.simplilearn.bigdata.casestudy_6

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

object Solution_3 {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.out.println("Please provide <hbase_namenodes_config>")
      System.exit(0)
    }
    val namenodes = args(0)
    getData("2014",namenodes)
  }

  def getData(year: String, namenodes: String): Unit = {
    val connection = HbaseManager.hbaseConnection(namenodes)
    val rowKey = Bytes.toBytes(year)
    val get:Get = new Get(rowKey)
    val table = connection.getTable(TableName.valueOf("total_traffic_accidents_yearly"))
    val result = table.get(get)
    if (result.size != 0) {
      val data = result.getValue(Bytes.toBytes("total"), Bytes.toBytes("count"));
      System.out.println("Result = "+Bytes.toString(data))
    }
  }
}
