package com.base.reduction;

import com.demo.wordcount.WordCountMapper;
import com.demo.wordcount.WordCountReduer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 归约案例
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordCount");

        job.setJarByClass(JobMain.class);
        //第二部：指定map阶段的处理方式和数据类型
        job.setMapperClass(WordCountMapper.class);
        //设置Map阶段K2的类型
        job.setMapOutputKeyClass(Text.class);
        //设置Map阶段V2的类型
        job.setMapOutputValueClass(LongWritable.class);

        //第三步：分区
        //第四步：排序

        //第五步：归约
        job.setCombinerClass(Combiner.class);
        //第六步：分组


        job.setReducerClass(WordCountReduer.class);
        //设置K3的类型
        job.setOutputKeyClass(Text.class);
        //设置V3的类型
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("input/ex/wordcount.txt"));
        Path path = new Path("output/ex/WordCount");
        FileOutputFormat.setOutputPath(job, path);

//        FileInputFormat.addInputPath(job, new Path("hdfs://master:8020/input/wordcount.txt"));
//        Path path = new Path("hdfs://master:8020/output/WordCount");
//        FileOutputFormat.setOutputPath(job, path);

        path.getFileSystem(conf).delete(path, true);
        if (job.waitForCompletion(true)) {
            System.out.println("ok");
        }
    }
}
