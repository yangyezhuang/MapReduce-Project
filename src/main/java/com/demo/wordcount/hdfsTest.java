package com.demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class hdfsTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS","hdfs://com.hadoop:8020");
        //conf.set("mapreduce.framework.name","yarn");
        //conf.set("yarn.resourcemanager.hostname","com.hadoop");
        Job job = Job.getInstance(conf,"wordcount");
        job.setJarByClass(WordCountTest.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job,new Path("hdfs://com.hadoop:8020/input/test.txt"));
        Path path = new Path("/output");
        FileOutputFormat.setOutputPath(job,path);

        path.getFileSystem(conf).delete(path,true);
        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
