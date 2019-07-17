package com.com.simplilearn.bigdata.spark

import java.net.{MalformedURLException, URL}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, TableDescriptorBuilder, _}

object HbaseManager {

  private var connection:Connection = null

  def getHbaseConfiguration(hbaseConfResources: String): Configuration = {
    val hbaseConf = new Configuration()
    for (confResource <- hbaseConfResources.split(",")) {
      try
        hbaseConf.addResource(new URL(confResource))
      catch {
        case e: MalformedURLException =>
          throw new RuntimeException(e)
      }
    }
    hbaseConf
  }

  @throws[Exception]
  def hbaseConnection(namenodeConfig: String): Connection = {
    if (connection == null) {
      val conf = getHbaseConfiguration(namenodeConfig)
      connection = ConnectionFactory.createConnection(conf)
      createTable()
    }
    connection
  }

  @throws[Exception]
  def createTable(): Unit = {
    val admin = connection.getAdmin
    var tableDescriptor: TableDescriptor = null
    val tablesNames = admin.listTableNames.toStream.map(record => (record.getName.map(_.toChar)).mkString ).toList;
    if (!tablesNames.contains("brand_order_count_monthly")) {
      tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("brand_order_count_monthly")).setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("brand_stats".getBytes).build).build
      admin.createTable(tableDescriptor)
    }
    if (!tablesNames.contains("category_order_count_monthly")) {
      tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("category_order_count_monthly")).setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("category_stats".getBytes).build).build
      admin.createTable(tableDescriptor)
    }
    if (!tablesNames.contains("brand_total_revenue_monthly")) {
      tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("brand_total_revenue_monthly")).setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("brand_stats".getBytes).build).build
      admin.createTable(tableDescriptor)
    }
    if (!tablesNames.contains("category_total_revenue_monthly")) {
      tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("category_total_revenue_monthly")).setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("category_stats".getBytes).build).build
      admin.createTable(tableDescriptor)
    }
  }
}
