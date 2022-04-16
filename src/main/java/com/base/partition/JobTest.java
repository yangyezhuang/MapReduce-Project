package com.base.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 分区案例
 */
public class JobTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Partitioner");

        job.setJarByClass(JobTest.class);
        //指定Mapper类（k2，v2）
        job.setMapperClass(PartMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //指定分区类
        job.setPartitionerClass(MyPartitioner.class);
        //指定Reducer类（k3，v3）
        job.setReducerClass(PartReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置ReduceTask个数，即分区个数
        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path("input/work/douban/part-r-00000"));
        Path path = new Path("output/ex/Partition");
        FileOutputFormat.setOutputPath(job, path);

        path.getFileSystem(conf).delete(path, true);
        if (job.waitForCompletion(true)) {
            System.out.println("ok");
        }
    }
}
