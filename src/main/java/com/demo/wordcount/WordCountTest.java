package com.demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 测试类
 */
public class WordCountTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();   //实例化配置
        Job job = Job.getInstance(conf, "wordcount");   //创建一个job任务对象
        //job.setJarByClass(WordCountTest.class);   //打包出错，加此配置

        job.setMapperClass(WordCountMapper.class);  //指定Map阶段的处理方式和数据类型
        job.setMapOutputKeyClass(Text.class);   //指定K1的类型
        job.setMapOutputValueClass(IntWritable.class);  //指定v1的类型


        job.setReducerClass(WordCountReduer.class);     //指定Reduce阶段的处理方式和数据类型
        job.setOutputKeyClass(Text.class);      //指定K2的类型
        job.setOutputValueClass(LongWritable.class);    //指定v2的类型
        //job.setNumReduceTasks(1);     //设置task个数

        FileInputFormat.addInputPath(job, new Path("input/test.txt"));
        Path path = new Path("output/wordCount");
        FileOutputFormat.setOutputPath(job, path);
        path.getFileSystem(conf).delete(path, true);

        //启动job任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
