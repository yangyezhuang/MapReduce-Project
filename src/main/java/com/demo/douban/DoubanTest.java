package com.projects.douban;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DoubanTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "douban");

        job.setJarByClass(DoubanTest.class);

        job.setMapperClass(DoubanMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(DoubanReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

//        FileInputFormat.addInputPath(job, new Path("hdfs://master:8020/input/douban.json"));
//        Path path = new Path("hdfs://master:8020/output/douban");
//        FileOutputFormat.setOutputPath(job, path);


        FileInputFormat.addInputPath(job, new Path("input/douban.json"));
        Path path = new Path("output/douban");
        FileOutputFormat.setOutputPath(job, path);


        path.getFileSystem(conf).delete(path, true);
        if (job.waitForCompletion(true)) {
            System.out.println("无效数据：" + job.getCounters().findCounter(DoubanCount.TotalRecorder).getValue() + "条");
        }
    }
}
