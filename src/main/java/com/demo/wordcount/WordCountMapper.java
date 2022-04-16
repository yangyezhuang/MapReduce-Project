package com.demo.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.String;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text text = new Text();
    IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(value.toString());
        // 将一行数据转换为字符串
        String line = value.toString();
        // 对字符串进行分割，放入数组
        String[] words = line.split(",");

        // 遍历数组里面的字符
        for (String word : words) {
            text.set(word);
            context.write(text, intWritable);
        }
    }
}
