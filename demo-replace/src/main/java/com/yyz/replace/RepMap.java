package com.yyz.replace;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RepMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text text = new Text();
    NullWritable nullWritable = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",");

        text.set(words[0]);
        context.write(text, nullWritable);
    }
}
