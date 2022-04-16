package com.projects.replace;
/*
去重
 */

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RepTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //conf.set("fs.default.name", "local");

        //创建一个job任务对象（去重）
        //Job job = Job.getInstance(conf, "Replace");
        Job job = Job.getInstance(conf);
        //打包出错，加此配置

        job.setJarByClass(RepTest.class);
        //指定Map阶段的处理方式和数据类型
        job.setMapperClass(RepMap.class);
        //      指定K1的类型
        job.setMapOutputKeyClass(Text.class);
        //      指定v1的类型
        job.setMapOutputValueClass(NullWritable.class);

        //指定Reduce阶段的处理方式和数据类型
        job.setReducerClass(RepReduce.class);
        //      指定K2的类型
        job.setOutputKeyClass(Text.class);
        //      指定v2的类型
        job.setOutputValueClass(NullWritable.class);
        //job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path("input/test.txt"));
        Path path = new Path("output/replace");
        FileOutputFormat.setOutputPath(job, path);
        path.getFileSystem(conf).delete(path, true);
        //启动job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
