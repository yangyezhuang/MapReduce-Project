package com.yyz.course;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CourseMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CourseJob");
        job.setJarByClass(Job.class);

        job.setMapperClass(CourseMapper.class);
        job.setReducerClass(CourseReducer.class);
        job.setPartitionerClass(CoursePartitioner.class);
        job.setNumReduceTasks(4);

        job.setMapOutputKeyClass(CourseBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(CourseBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("demo-course/input"));
        Path path = new Path("demo-course/output");
        FileOutputFormat.setOutputPath(job, path);
        path.getFileSystem(conf).delete(path, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
