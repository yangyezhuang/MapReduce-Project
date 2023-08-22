package com.yyz.partitionEmployee;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class partEmployeeMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(partEmployeeMain.class);

        job.setMapperClass(partEmployeeMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Employee.class);

        job.setPartitionerClass(MyEmployeeParitioner.class);
        job.setNumReduceTasks(3);

        job.setReducerClass(partEmployeeReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Employee.class);

        FileInputFormat.addInputPath(job, new Path("demo-partition-employee/input/emp.csv"));
        Path path = new Path("demo-partition-employee/output");
        FileOutputFormat.setOutputPath(job, path);
        path.getFileSystem(conf).delete(path, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
