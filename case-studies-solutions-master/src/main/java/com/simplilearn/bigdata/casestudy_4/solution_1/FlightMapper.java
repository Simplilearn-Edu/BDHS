package com.simplilearn.bigdata.casestudy_4.solution_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class FlightMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    private final String SEPERATOR = "-";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            String arr[] = line.split(",");
            int canceled = 0;
            try {
                if (arr[10].trim().length() != 0) {
                    canceled = Integer.parseInt(arr[10]);
                }
                context.write(new Text(arr[0] + SEPERATOR + arr[1]), new IntWritable(canceled));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
