package com.yyz.course;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CourseMapper extends Mapper<LongWritable, Text, CourseBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");

        int number = split.length - 2;
        float score = 0;

        for (int i = 2; i < split.length; i++) {
            score += Float.parseFloat(split[i]);
        }

        score = score / number;
        CourseBean courseBean = new CourseBean(split[0], split[1], score);
        context.write(courseBean, NullWritable.get());
    }
}
