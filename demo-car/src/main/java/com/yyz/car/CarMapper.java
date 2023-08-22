package com.yyz.car;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class CarMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String info = value.toString();

        if (info.equals("")) {
            return;
        }

        String[] ll = info.split(",");
        int[] ls = {5, 7, 12, 14};

        for (int i : ls) {
            if (ll[i].contains("--")) {
                context.getCounter(CountEnum.TotalRecorder).increment(1);
                return;
            }
        }

        text.set(info);
        context.write(text, new IntWritable(1));
    }
}






