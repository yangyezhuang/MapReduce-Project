package com.yyz.reduction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 归约案例
 */
public class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text text = new Text();
    //    IntWritable intwritable = new IntWritable();
    LongWritable longWritable = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //  方法1：计数器
        Counter counter = context.getCounter("MR_COUNTER", "part_counter");
        counter.increment(1L);

        String[] words = value.toString().split(",");
        for (String word : words) {
            text.set(word);
            longWritable.set(1);
            context.write(text, longWritable);
        }
    }
}
