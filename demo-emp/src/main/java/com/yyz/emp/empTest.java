package com.yyz.emp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class empTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "csvTest");

        job.setJarByClass(empTest.class);
        //Mapper类，设置k2，v2
        job.setMapperClass(empMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 分区类
        job.setPartitionerClass(empPartition.class);
        // Reduce类，设置k3，v3
        job.setReducerClass(empReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置分区数
        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path("demo-emp/input/emp.csv"));
        Path path = new Path("demo-emp/output");
        FileOutputFormat.setOutputPath(job, path);

        path.getFileSystem(conf).delete(path, true);
        if (job.waitForCompletion(true)) {
            System.out.println("ok");
        }

    }
}
