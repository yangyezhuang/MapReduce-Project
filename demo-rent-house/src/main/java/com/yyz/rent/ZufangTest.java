package com.yyz.rent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ZufangTest {
//    public static int COUNT = 0;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RentCount");

        job.setJarByClass(ZufangTest.class);
        job.setMapperClass(ZufangMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(ZufangReduce.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);


//        FileInputFormat.addInputPath(job, new Path("hdfs://master:8020/input/zufang"));
//        Path path = new Path("hdfs://master:8020/output/zufang");
//        FileOutputFormat.setOutputPath(job, path);

        FileInputFormat.addInputPath(job, new Path("demo-rent-house/input"));
        Path path = new Path("demo-rent-house/output");
        FileOutputFormat.setOutputPath(job, path);


        path.getFileSystem(conf).delete(path, true);
        if (job.waitForCompletion(true)) {
            System.out.println("Total num: " + job.getCounters().findCounter(count.TotalRecorder).getValue());
//            System.out.println("Total num: " + COUNT);
        }
    }
}
