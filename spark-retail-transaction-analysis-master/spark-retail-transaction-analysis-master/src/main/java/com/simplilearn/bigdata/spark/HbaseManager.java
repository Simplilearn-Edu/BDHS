package com.simplilearn.bigdata.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

final class HbaseManager {
    private static final HbaseManager INSTANCE = new HbaseManager();

    private static Connection connection = null;

    private HbaseManager() {

    }

    static HbaseManager getInstance() {
        return INSTANCE;
    }

    Configuration getHbaseConfiguration(String hbaseConfResources) {
        Configuration hbaseConf = new Configuration();
        for (String confResource : hbaseConfResources.split(",")) {
            try {
                hbaseConf.addResource(new URL(confResource));
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }
        return hbaseConf;
    }

    public static Connection hbaseConnection(String namenodeConfig) throws Exception {
        if (connection == null) {
            Configuration conf = HbaseManager.getInstance().getHbaseConfiguration(namenodeConfig);
            connection = ConnectionFactory.createConnection(conf);
            createTable();
        }
        return connection;
    }

    public static void createTable() throws Exception{
        Admin admin = connection.getAdmin();
        TableDescriptor tableDescriptor = null;
        List<String> tablesNames = Arrays.asList(admin.listTableNames()).stream().map(table -> new String(table.getName())).collect(Collectors.toList());
        if(!tablesNames.contains("brand_order_count_monthly")) {
            tableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf("brand_order_count_monthly"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("brand_stats".getBytes()).build())
                    .build();

            admin.createTable(tableDescriptor);
        }

        if(!tablesNames.contains("category_order_count_monthly")) {
            tableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf("category_order_count_monthly"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("category_stats".getBytes()).build())
                    .build();

            admin.createTable(tableDescriptor);
        }

        if(!tablesNames.contains("brand_total_revenue_monthly")) {
            tableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf("brand_total_revenue_monthly"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("brand_stats".getBytes()).build())
                    .build();

            admin.createTable(tableDescriptor);
        }

        if(!tablesNames.contains("category_total_revenue_monthly")) {
            tableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf("category_total_revenue_monthly"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("category_stats".getBytes()).build())
                    .build();

            admin.createTable(tableDescriptor);
        }

    }
}
