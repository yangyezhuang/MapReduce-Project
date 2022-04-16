package com.demo.Course;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CourseReducer extends Reducer<CourseBean, NullWritable, CourseBean, NullWritable> {
    @Override
    protected void reduce(CourseBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
