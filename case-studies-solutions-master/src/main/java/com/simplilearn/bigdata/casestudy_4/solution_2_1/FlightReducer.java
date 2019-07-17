package com.simplilearn.bigdata.casestudy_4.solution_2_1;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FlightReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {

    private static Map<String, String> airlineCodeNameMap = new HashMap<>();


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            sum += iterator.next().get();
        }
        URI[] paths = context.getCacheFiles();
        if (paths.length > 0) {
            contructAircodeAndNameMap(paths[0].toString(), context);
        }
        context.write(new Text(airlineCodeNameMap.get(key.toString())), new IntWritable(sum));
    }


    private void contructAircodeAndNameMap(String file, Context context) {
        String strRead;
        BufferedReader reader = null;
        try {
            try {
                reader = new BufferedReader(new FileReader("./some"));
            }catch(Exception ex) {
                try {
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path path = new Path(file);
                    reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                }catch (Exception ex1) {
                    //for local
                    reader = new BufferedReader(new FileReader(file));
                }

            }
            while ((strRead = reader.readLine()) != null) {
                if(strRead.indexOf("IATA_CODE") == -1) {
                    String splitarray[] = strRead.split(",");
                    airlineCodeNameMap.put(splitarray[0].trim(), splitarray[1].trim());
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
