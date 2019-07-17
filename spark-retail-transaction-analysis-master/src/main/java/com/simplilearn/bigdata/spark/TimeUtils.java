package com.simplilearn.bigdata.spark;

import java.sql.Timestamp;
import java.util.Calendar;

public class TimeUtils {

    private static String[] monthName = {"January", "February",
            "March", "April", "May", "June", "July",
            "August", "September", "October", "November",
            "December"};

    private static String[] quaters = {"1st Quarter", "2nd Quarter",
            "3rd Quarter", "4th Quarter"};

    public static Integer getMonth(Timestamp timestamp) {
        long time = timestamp.getTime();
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        return cal.get(Calendar.MONTH);
    }

    public static Integer getYear(Timestamp timestamp) {
        long time = timestamp.getTime();
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        return cal.get(Calendar.YEAR);
    }

    public static String numberToMonthName(int number) {
        return monthName[number];
    }

    public static String numberToQuarter(int number) {
        return quaters[number-1];
    }

    public static Integer getQuater(Timestamp timestamp) {
        long time = timestamp.getTime();
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        return (cal.get(Calendar.MONTH) / 3) + 1;
    }
}
