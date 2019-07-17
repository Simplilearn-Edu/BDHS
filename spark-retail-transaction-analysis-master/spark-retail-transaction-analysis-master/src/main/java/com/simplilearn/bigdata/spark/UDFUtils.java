package com.simplilearn.bigdata.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

public class UDFUtils {

    private static DecimalFormat formatter = null;

    static {
        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
        decimalFormatSymbols.setDecimalSeparator('.');
        decimalFormatSymbols.setGroupingSeparator(',');
        formatter = new DecimalFormat("#,##0.00", decimalFormatSymbols);
    }

    public static UDF1 toMonth = new UDF1<Timestamp, Integer>() {
        public Integer call(final Timestamp timestamp) throws Exception {
            return TimeUtils.getMonth(timestamp);
        }
    };

    public static UDF1 toYear = new UDF1<Timestamp, Integer>() {
        public Integer call(final Timestamp timestamp) throws Exception {
            return TimeUtils.getYear(timestamp);
        }
    };

    public static UDF1 toQuarter = new UDF1<Timestamp, Integer>() {
        public Integer call(final Timestamp timestamp) throws Exception {
            return TimeUtils.getQuater(timestamp);
        }
    };

    public static UDF1 toMonthName = new UDF1<Integer, String>() {
        public String call(final Integer monthNumber) throws Exception {
            return TimeUtils.numberToMonthName(monthNumber);
        }
    };

    public static UDF1 toQuarterName = new UDF1<Integer, String>() {
        public String call(final Integer quarterNum) throws Exception {
            return TimeUtils.numberToQuarter(quarterNum);
        }
    };

    public static UDF1 doubleToString = new UDF1<Double, String>() {
        public String call(final Double totalAmount) throws Exception {
            return formatter.format(totalAmount);
        }
    };

    public static UDF2 calculateProfit = new UDF2<Integer, Double, Double>() {
        public Double call(final Integer marginPercentage, final Double transactionAmount) throws Exception {
            return transactionAmount * (marginPercentage/100.0);
        }
    };


    public static void registerUDFs(SparkSession sparkSession) {
        sparkSession.udf().register("toMonth", UDFUtils.toMonth, DataTypes.IntegerType);
        sparkSession.udf().register("toYear", UDFUtils.toYear, DataTypes.IntegerType);
        sparkSession.udf().register("toMonthName", UDFUtils.toMonthName, DataTypes.StringType);

        sparkSession.udf().register("toQuarter", UDFUtils.toQuarter, DataTypes.IntegerType);
        sparkSession.udf().register("toQuarterName", UDFUtils.toQuarterName, DataTypes.StringType);

        sparkSession.udf().register("doubleToString", UDFUtils.doubleToString, DataTypes.StringType);

        sparkSession.udf().register("calculateProfit", UDFUtils.calculateProfit, DataTypes.DoubleType);

    }
}
