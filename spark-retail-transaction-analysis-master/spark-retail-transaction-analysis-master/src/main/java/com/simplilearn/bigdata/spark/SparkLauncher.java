package com.simplilearn.bigdata.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

public class SparkLauncher {

    private static final Logger logger = LoggerFactory.getLogger(SparkLauncher.class);

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Please provide <input_path> <spark_master>");
            System.exit(0);
        }

        String inputPath = args[0];
        String master = args[1];

        SparkSession sparkSession = getSparkSession("spark-retail-transaction-analysis", master);
//        unionAllTransactions(sparkSession);
        Dataset<Row> dataset = readFile(inputPath,readWithHeader(sparkSession));

        topFiveBrandsSoldMost(dataset);
        topFiveProductsSoldMost(dataset);
        productCountAndRevenueQuarterly(dataset);
        productCountQuarterlyByCategory(dataset);
        productCountQuarterlyByBrand(dataset);
        topFiveProfitableProductsSold(dataset);
        topFiveProfitableBrandCategorywise(dataset);
        bottomFiveeBrandByOrderCountAndCategorywise(dataset);
        topFiveProfitableProductsSoldMonthly(dataset);
        bottomFiveLeastSellingProductsSoldMonthly(dataset);
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
     * Solution
     *  5.a.i.1
     * @param dataset
     */
    private static void topFiveBrandsSoldMost(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Month", functions.callUDF("toMonth", dataset.col("InvoiceDate")));
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});

        dataset = dataset.filter("SubCategory='mobile'").filter("month > 9 OR month = 0");
        print(new String[] {"Row Count"}, new Object[] {dataset.count()});
        dataset = dataset.select("month", "brand", "Quantity").groupBy("month", "brand").agg(functions.sum("Quantity").as("OrderCount"));

        List<Integer> months = Arrays.asList(0,10, 11);
        for(int i=0 ;i < months.size(); i++) {
            Dataset<Row> topBrandsSold = dataset.filter("month = "+months.get(i)).sort(functions.desc("OrderCount")).limit(5)
                    .withColumn("MonthName", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print("Top 5 Brands having maximum Order in mobile category ",topBrandsSold.collectAsList().toArray(), topBrandsSold.columns());
        }
    }

    /**
     * Solution
     *  5.a.i.2
     * @param dataset
     */
    private static void topFiveProductsSoldMost(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Month", functions.callUDF("toMonth", dataset.col("InvoiceDate")));
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});

        dataset = dataset.filter("month > 9 OR month = 0");
        print(new String[] {"Row Count"}, new Object[] {dataset.count()});
        dataset = dataset.select("month", "ProductCode", "Description", "Quantity").groupBy("month", "ProductCode", "Description").agg(functions.sum("Quantity").as("OrderCount"));

        List<Integer> months = Arrays.asList(0,10, 11);
        for(int i=0 ;i < months.size(); i++) {
            Dataset<Row> topProductsSold = dataset.filter("month = "+months.get(i)).sort(functions.desc("OrderCount")).limit(5)
                    .withColumn("MonthName", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print("Top 5 products having maximum Order in mobile category ",topProductsSold.collectAsList().toArray(), topProductsSold.columns());
        }
    }

    /**
     * Solution
     *  5.a.ii.1
     * @param dataset
     */
    private static void productCountAndRevenueQuarterly(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Quarter", functions.callUDF("toQuarter", dataset.col("InvoiceDate")));
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});
        dataset = dataset.select("Quarter", "TransactionAmount", "Quantity").groupBy("Quarter").agg(functions.sum("Quantity").as("Inventory Sold"),
                functions.sum("TransactionAmount").as("Total Revenue"));
        Dataset<Row> productStatsQuaterly = dataset
                .withColumn("Quarter Name", functions.callUDF("toQuarterName", dataset.col("Quarter")))
                .withColumn("Total Revenue", functions.callUDF("doubleToString", dataset.col("Total Revenue")))
                .drop("Quarter");
        print("Quarterly revenue and sold inventory ",productStatsQuaterly.collectAsList().toArray(), productStatsQuaterly.columns());
    }

    /**
     * Solution
     *  5.a.ii.2
     * @param dataset
     */
    private static void productCountQuarterlyByCategory(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Quarter", functions.callUDF("toQuarter", dataset.col("InvoiceDate")));
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});
        dataset = dataset.select("Quarter",  "Category", "Quantity").groupBy("Quarter", "Category").agg(functions.sum("Quantity").as("Inventory Sold"));
        Dataset<Row> productStatsByCategoryQuaterly = dataset
                .withColumn("Quarter Name", functions.callUDF("toQuarterName", dataset.col("Quarter")))
                .drop("Quarter");
        print("Quarterly sold inventory for each Category",productStatsByCategoryQuaterly.collectAsList().toArray(), productStatsByCategoryQuaterly.columns());
    }

    /**
     * Solution
     *  5.a.ii.3
     * @param dataset
     */
    private static void productCountQuarterlyByBrand(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Quarter", functions.callUDF("toQuarter", dataset.col("InvoiceDate")));
        print(new String[] {"Dataset Schema"}, new Object[] {dataset.schema()});
        dataset = dataset.select("Quarter",  "Brand", "Quantity").groupBy("Quarter", "Brand").agg(functions.sum("Quantity").as("Inventory Sold"));
        Dataset<Row> productStatsByBrandQuaterly = dataset
                .withColumn("Quarter Name", functions.callUDF("toQuarterName", dataset.col("Quarter")))
                .drop("Quarter");
        print("Quarterly sold inventory for each Brand",productStatsByBrandQuaterly.collectAsList().toArray(), productStatsByBrandQuaterly.columns());
    }

    /**
     * Solution
     *  5.a.iii.1
     * @param dataset
     */
    private static void topFiveProfitableProductsSold(Dataset<Row> dataset) {
        dataset = dataset.select("ProductCode", "Description", "Marginpercentage", "TransactionAmount")
                            .withColumn("Profit", functions.callUDF("calculateProfit", dataset.col("Marginpercentage"), dataset.col("TransactionAmount")))
                            .drop("Marginpercentage", "TransactionAmount")
                            .groupBy("ProductCode", "Description")
                            .agg(functions.sum("Profit").as("Profit"))
                            .sort(functions.desc("Profit"))
                            .limit(5);
        dataset = dataset.withColumn("Profit", functions.callUDF("doubleToString", dataset.col("Profit")));
        print("Top 5 products profitable products ",dataset.collectAsList().toArray(), dataset.columns());
    }

    /**
     * Solution
     *  5.a.iv.1
     * @param dataset
     */
    private static void topFiveProfitableBrandCategorywise(Dataset<Row> dataset) {
        dataset = dataset.select("Category", "SubCategory", "Brand", "Marginpercentage", "TransactionAmount")
                .withColumn("Profit", functions.callUDF("calculateProfit", dataset.col("Marginpercentage"), dataset.col("TransactionAmount")))
                .drop("Marginpercentage", "TransactionAmount")
                .groupBy("Category", "SubCategory", "Brand")
                .agg(functions.sum("Profit").as("Profit"))
                .sort(functions.desc("Profit"))
                .limit(5);

        dataset = dataset.withColumn("Profit", functions.callUDF("doubleToString", dataset.col("Profit")));
        print("Top 5 profitable brands category wise ",dataset.collectAsList().toArray(), dataset.columns());
    }

    /**
     * Solution
     *  5.a.iv.2
     * @param dataset
     */
    private static void bottomFiveeBrandByOrderCountAndCategorywise(Dataset<Row> dataset) {
        dataset = dataset.select("Category", "SubCategory", "Brand", "Quantity")
                .groupBy("Category", "SubCategory", "Brand")
                .agg(functions.sum("Quantity").as("Quantity"))
                .sort(functions.asc("Quantity")).limit(5);
        print("Top 5 brands category wise which has less orders ",dataset.collectAsList().toArray(), dataset.columns());
    }

    /**
     * Solution
     *  5.a.v.1
     * @param dataset
     */
    private static void topFiveProductByOrderCount(Dataset<Row> dataset) {
        dataset = dataset.select("ProductCode", "Description", "Quantity")
                .groupBy("ProductCode", "Description")
                .agg(functions.sum("Quantity").as("Quantity"))
                .sort(functions.desc("Quantity")).limit(5);


        dataset = dataset.withColumn("Month", functions.callUDF("toMonth", dataset.col("InvoiceDate")));
        dataset = dataset.select("Month", "ProductCode", "Description", "Quantity")
                .groupBy("Month", "ProductCode", "Description")
                .agg(functions.sum("Quantity").as("Quantity"));
        for(int i=0 ;i < 12; i++) {
            Dataset<Row> topProductsSoldMonthly = dataset.filter("month = "+i).sort(functions.desc("Quantity")).limit(5)
                    .withColumn("MonthName", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print("Top 5 product which has maximum orders = ",topProductsSoldMonthly.collectAsList().toArray(), topProductsSoldMonthly.columns());
        }
    }

    /**
     * Solution
     *  5.a.v.2
     * @param dataset
     */
    private static void topFiveProfitableProductsSoldMonthly(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Month", functions.callUDF("toMonth", dataset.col("InvoiceDate")));
        dataset = dataset.select("Month", "ProductCode", "Description", "Marginpercentage", "TransactionAmount")
                .withColumn("Profit", functions.callUDF("calculateProfit", dataset.col("Marginpercentage"), dataset.col("TransactionAmount")))
                .drop("Marginpercentage", "TransactionAmount")
                .groupBy("Month", "ProductCode", "Description")
                .agg(functions.sum("Profit").as("Profit"));
        for(int i=0 ;i < 12; i++) {
            Dataset<Row> topProductsSoldMonthly = dataset.filter("month = "+i).sort(functions.desc("Profit"))
                    .withColumn("Profit", functions.callUDF("doubleToString", dataset.col("Profit"))).limit(5)
                    .withColumn("MonthName", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print("Top 5 products profitable products monthly ",topProductsSoldMonthly.collectAsList().toArray(), topProductsSoldMonthly.columns());
        }
    }

    /**
     * Solution
     *  5.a.v.3
     * @param dataset
     */
    private static void bottomFiveLeastSellingProductsSoldMonthly(Dataset<Row> dataset) {
        dataset = dataset.withColumn("Month", functions.callUDF("toMonth", dataset.col("InvoiceDate")));
        dataset = dataset.select("Month", "ProductCode", "Description", "Quantity")
                .groupBy("Month", "ProductCode", "Description")
                .agg(functions.sum("Quantity").as("Quantity"));
        for(int i=0 ;i < 12; i++) {
            Dataset<Row> leastSellingProductsMonthly = dataset.filter("month = "+i).sort(functions.asc("Quantity")).limit(5)
                    .withColumn("MonthName", functions.callUDF("toMonthName", dataset.col("Month"))).drop("Month");
            print("Botton 5 products least selling products monthly ",leastSellingProductsMonthly.collectAsList().toArray(), leastSellingProductsMonthly.columns());
        }
    }

    @SuppressWarnings("unused")
    private static void unionAllTransactions(SparkSession sparkSession) {
//        String filePaths[] = {
//                "/Users/Downloads/dataset/all_electronics_transaction.csv",
//                "/Users/Downloads/dataset/all_men_transaction.csv",
//                "/Users/Downloads/dataset/all_women_transaction.csv",
//                "/Users/Downloads/dataset/all_mobile_phones_transaction.csv"
//        };

        String filePaths[] = {
                "/Users/Downloads/dataset/complete/all_transaction_1.csv"
        };
        String outputPath = "/Users/Downloads/dataset/complete_1";
        unionAllFiles(sparkSession, filePaths, outputPath);
    }

    @SuppressWarnings("unused")
    private static void unionAllFiles(SparkSession sparkSession, String[] filePaths, String outputPath){
        Dataset<Row> dataset = null;
        for(String filePath : filePaths) {
            Dataset<Row> dataset1 = readFile(filePath,readWithHeader(sparkSession));
            if(dataset == null ){
                dataset = dataset1;
            } else {
                dataset = dataset.union(dataset1);
            }
        }

        dataset.coalesce(1)
                .withColumnRenamed("Product Code","ProductCode")
                .withColumnRenamed("Actual Price","ActualPrice")
                .withColumnRenamed("Margin percentage","Marginpercentage")
                .withColumnRenamed("Discount percentage","Discountpercentage")
                .withColumnRenamed("Transaction Amount","TransactionAmount")
                .withColumnRenamed("Invoice Date","InvoiceDate")
                .withColumnRenamed("Invoice Number","InvoiceNumber")
                .withColumnRenamed("Customer Id","CustomerId")
                .sort(functions.asc("InvoiceDate"))
                .write()
                .format("csv")
                .option("header", "true")
                .save(outputPath);
    }

    /**
     * This are just helper methods. Not used anywhere
     * @param dataset
     */
    @SuppressWarnings("unused")
    private static void findDuplicateProductCodes(Dataset<Row> dataset) {
        List<Row> rows = dataset.select("Description","ProductCode").groupBy("Description").agg(functions.collect_list("ProductCode").as("ProductCode")).drop("Description").collectAsList();
        final List<String> sb = new ArrayList<>();
        final List<Integer> count = new ArrayList<Integer>();
        rows.forEach(new Consumer<Row>() {
            @Override
            public void accept(Row row) {
                Map<String,Integer> countMap = new HashMap<>();
                for(Object object : row.getList(0).toArray()) {
                    if(countMap.get(object.toString()) == null) {
                        countMap.put(object.toString(), 1);
                    } else{
                        countMap.put(object.toString(), 1 + countMap.get(object.toString()));
                    }
                }

                if(countMap.size() > 1) {
                    // Create a list from elements of HashMap
                    List<Map.Entry<String, Integer> > list =
                            new LinkedList<Map.Entry<String, Integer> >(countMap.entrySet());

                    // Sort the list
                    Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() {
                        public int compare(Map.Entry<String, Integer> o1,
                                           Map.Entry<String, Integer> o2)
                        {
                            return (o1.getValue()).compareTo(o2.getValue());
                        }
                    });

                    for(int i =1 ; i < list.size() ; i++) {
                        if(count.size() == 0){
                            count.add(list.get(i).getValue());
                        }else{
                            int total = count.get(0) + list.get(i).getValue();
                            count.clear();
                            count.add(total);
                        }
                        sb.add(list.get(0).getKey());

                    }
                }
            }
        });

        print(new String[] {"duplicate product code Size "}, new String[] {sb.size()+""});
        print(new String[] {"duplicate product code count "}, new String[] {count+""});
    }
}
