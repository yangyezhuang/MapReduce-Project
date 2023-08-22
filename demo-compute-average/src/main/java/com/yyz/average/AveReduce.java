package com.yyz.average;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AveReduce extends Reducer<Text, IntWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for (IntWritable value : values) {
            //每个元素求和sum
            sum += value.get();
            //统计元素的次数count
            count++;
        }
        //统计次数
        int ave = sum / count;
//        IntWritable aver = new IntWritable();
//        aver.set(ave);
//        context.write(key,aver);
        context.write(key, new LongWritable(ave));
    }
}
