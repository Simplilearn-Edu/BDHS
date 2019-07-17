package com.simplilearn.bigdata.spark;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistSparkLauncher {

    private static final Logger logger = LoggerFactory.getLogger(PersistSparkLauncher.class);
    private static final String SEPERATOR = "-";

    public static void main(String[] args) throws Exception{

        if (args.length != 3) {
            System.out.println("Please provide <input_path> <spark_master> <namenode_config>");
            System.exit(0);
        }

        String inputPath = args[0];
        String master = args[1];
        String namenodes = args[2];

        SparkSession sparkSession = getSparkSession("spark-retail-transaction-analysis-hbase-monthly", master);
        Dataset<Row> dataset = readFile(inputPath ,readWithHeader(sparkSession));

        Connection connection = HbaseManager.hbaseConnection(namenodes);
        persistOrderCountByBrandMonthly(dataset, namenodes);
        persistRevenueByBrandMonthly(dataset, namenodes);
        persistOrderCountByCategoryMonthly(dataset, namenodes);
        persistRevenueByCategoryMonthly(dataset, namenodes);

    }

    private static SparkSession getSparkSession(String appName, String master) {
        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .master(master.equalsIgnoreCase("local") ? "local[*]" : master)
                .getOrCreate();
        print(new String[] {"Spark version"}, new Object[] {sparkSession.version()});
        UDFUtils.registerUDFs(sparkSession);
        return sparkSession;
    }

    private static SparkContext getSparkContezxt(String appName, String master) {
        return getSparkSession(appName, master).sparkContext();
    }

    private static DataFrameReader readWithHeader(SparkSession sparkSession) {
        return sparkSession.read()
                .option("header",true)
                .option("inferSchema", true)
                .option("mode", "DROPMALFORMED");
    }

    private static void print(String[] key, Object[] values) {
        StringBuilder logBuilder = new StringBuilder();
        for(int i =0 ;i < key.length ; i++) {
            logBuilder.append(key[i] +" = "+values[i])
                    .append("\n");
        }
        System.out.println(logBuilder.toString());
    }

    private static void print(String key, Object[] values, Object[] colNames) {
        StringBuilder logBuilder = new StringBuilder();
        logBuilder.append(key +" = \n");
        for(int i =0 ;i < colNames.length ; i++) {
            logBuilder.append(colNames[i])
                    .append(", ");
        }
        logBuilder.append("\n");
        for(int i =0 ;i < values.length ; i++) {
            logBuilder.append(values[i])
                    .append("\n");
        }
        System.out.println(logBuilder.toString());
    }


    private static Dataset<Row> readFile(String path, DataFrameReader dataFrameReader) {
        print(new String[] {"Reading file "}, new Object[] {path});
        Dataset<Row> dataset = dataFrameReader.csv(path);
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});
        print(new String[] {"Row Count"}, new Object[] {dataset.count()});
        return dataset;
    }

    /**
     * @param dataset
     */
    private static void persistOrderCountByBrandMonthly(Dataset<Row> dataset, final String namenodes) {
        dataset = dataset
                .withColumn("Month", functions.callUDF("toMonth", dataset.col("InvoiceDate")))
                .withColumn("Year", functions.callUDF("toYear", dataset.col("InvoiceDate")));
        dataset = dataset
                .select("Year", "Month", "Brand", "Quantity")
                .groupBy("Year", "Month", "Brand")
                .agg(functions.sum("Quantity").as("Quantity"));
        for(int i=0 ;i < 12; i++) {
            Dataset<Row> orderCountByBrandMonthly = dataset
                    .filter("month = "+i)
                    .withColumn("MonthName", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print(new String[] {"Row Count"}, new Object[] {orderCountByBrandMonthly.count()});

            orderCountByBrandMonthly.foreach((ForeachFunction<Row>) row -> {
                Table table = null;
                try {
                    Connection connection = HbaseManager.hbaseConnection(namenodes);
                    byte[] rowKey = Bytes.toBytes(row.get(0).toString() + SEPERATOR + row.get(3).toString() + SEPERATOR + row.get(1).toString());
                    Get get = new Get(rowKey);
                    table = connection.getTable(TableName.valueOf("brand_order_count_monthly"));
                    Result result = table.get(get);
                    if(result.size() == 0) {
                        Put put = new Put(rowKey);
                        put.addColumn(Bytes.toBytes("brand_stats"), Bytes.toBytes("data"), Bytes.toBytes((((Long)row.get(2))).toString()));
                        table.put(put);
                    }
                }finally {
                    try{
                        table.close();
                    }catch(Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
    }

    /**
     * @param dataset
     */
    private static void persistRevenueByBrandMonthly(Dataset<Row> dataset, final String namenodes) {
        dataset = dataset
                .withColumn("Month", functions.callUDF("toMonth", dataset.col("InvoiceDate")))
                .withColumn("Year", functions.callUDF("toYear", dataset.col("InvoiceDate")));
        dataset = dataset
                .select("Year", "Month", "Brand", "TransactionAmount")
                .groupBy("Year", "Month", "Brand")
                .agg(functions.sum("TransactionAmount").as("TransactionAmount"));
        for(int i=0 ;i < 12; i++) {
            Dataset<Row> revenueByBrandMonthly = dataset
                    .filter("month = "+i)
                    .withColumn("MonthName", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print(new String[] {"Row Count"}, new Object[] {revenueByBrandMonthly.count()});

            revenueByBrandMonthly.foreach((ForeachFunction<Row>) row -> {
                Table table = null;
                try {
                    Connection connection = HbaseManager.hbaseConnection(namenodes);
                    byte[] rowKey = Bytes.toBytes(row.get(0).toString() + SEPERATOR + row.get(3).toString() + SEPERATOR + row.get(1).toString());
                    Get get = new Get(rowKey);
                    table = connection.getTable(TableName.valueOf("brand_total_revenue_monthly"));
                    Result result = table.get(get);
                    if(result.size() == 0) {
                        Put put = new Put(rowKey);
                        put.addColumn(Bytes.toBytes("brand_stats"), Bytes.toBytes("data"), Bytes.toBytes(((Double)row.get(2)).toString()));
                        table.put(put);
                    }
                }finally {
                    try{
                        table.close();
                    }catch(Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
    }

    /**
     * @param dataset
     */
    private static void persistOrderCountByCategoryMonthly(Dataset<Row> dataset, final String namenodes) {
        dataset = dataset
                .withColumn("Month", functions.callUDF("toMonth", dataset.col("InvoiceDate")))
                .withColumn("Year", functions.callUDF("toYear", dataset.col("InvoiceDate")));
        dataset = dataset
                .select("Year", "Month", "Category", "Quantity")
                .groupBy("Year", "Month", "Category")
                .agg(functions.sum("Quantity").as("Quantity"));
        for(int i=0 ;i < 12; i++) {
            Dataset<Row> orderCountByCategoryMonthly = dataset
                    .filter("month = "+i)
                    .withColumn("MonthName", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print(new String[] {"Row Count"}, new Object[] {dataset.count()});
            orderCountByCategoryMonthly.foreach((ForeachFunction<Row>) row -> {
                Table table = null;
                try {
                    Connection connection = HbaseManager.hbaseConnection(namenodes);
                    byte[] rowKey = Bytes.toBytes(row.get(0).toString() + SEPERATOR + row.get(3).toString() + SEPERATOR + row.get(1).toString());
                    Get get = new Get(rowKey);
                    table = connection.getTable(TableName.valueOf("category_order_count_monthly"));
                    Result result = table.get(get);
                    if(result.size() == 0) {
                        Put put = new Put(rowKey);
                        put.addColumn(Bytes.toBytes("category_stats"), Bytes.toBytes("data"), Bytes.toBytes((((Long)row.get(2)).toString())));
                        table.put(put);
                    }
                }finally {
                    try{
                        table.close();
                    }catch(Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
    }

    /**
     * @param dataset
     * @param namenodes
     */
    private static void persistRevenueByCategoryMonthly(Dataset<Row> dataset, final String namenodes) {
        dataset = dataset
                .withColumn("Month", functions.callUDF("toMonth", dataset.col("InvoiceDate")))
                .withColumn("Year", functions.callUDF("toYear", dataset.col("InvoiceDate")));
        dataset = dataset
                .select("Year", "Month", "Category", "TransactionAmount")
                .groupBy("Year", "Month", "Category")
                .agg(functions.sum("TransactionAmount").as("TransactionAmount"));
        for(int i=0 ;i < 12; i++) {
            Dataset<Row> revenueByCategoryMonthly = dataset
                    .filter("month = "+i)
                    .withColumn("MonthName", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print(new String[] {"Row Count"}, new Object[] {dataset.count()});
            revenueByCategoryMonthly.foreach((ForeachFunction<Row>) row -> {
                Table table = null;
                try {
                    Connection connection = HbaseManager.hbaseConnection(namenodes);
                    byte[] rowKey = Bytes.toBytes(row.get(0).toString() + SEPERATOR + row.get(3).toString() + SEPERATOR + row.get(1).toString());
                    Get get = new Get(rowKey);
                    table = connection.getTable(TableName.valueOf("category_total_revenue_monthly"));
                    Result result = table.get(get);
                    if(result.size() == 0) {
                        Put put = new Put(rowKey);
                        put.addColumn(Bytes.toBytes("category_stats"), Bytes.toBytes("data"), Bytes.toBytes(((Double)row.get(2)).toString()));
                        table.put(put);
                    }
                }finally {
                    try{
                        table.close();
                    }catch(Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
    }
}
