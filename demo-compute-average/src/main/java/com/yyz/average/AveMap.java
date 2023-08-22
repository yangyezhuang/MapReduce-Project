package com.yyz.average;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AveMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text text = new Text();
//    IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(value.toString());
        String line = value.toString();
        //对文件进行拆分
        String[] words = line.split("\t");
        //获取文件key值
        text.set(words[0]);
        //获取文件value值
        int click=Integer.parseInt(words[1]);
        //将key和value写进文本中
        context.write(text, new IntWritable(click));
//        int clicknum = Integer.parseInt(fields[1]);
//        context.write(new Text(fields[0]),new LongWritable(clicknum));
//        String[] str = value.toString().split(" ");
//        String name = str[0];
//        long a =Long.parseLong(str[str.length-1]);
//        context.write(new Text(name),new IntWritable(a));
    }
}
