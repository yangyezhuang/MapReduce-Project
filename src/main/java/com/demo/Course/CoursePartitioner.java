package com.projects.Course;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CoursePartitioner extends Partitioner<CourseBean, NullWritable> {

    @Override
    public int getPartition(CourseBean courseBean, NullWritable nullWritable, int i) {
        switch (courseBean.getCourse()) {
            case "computer":
                return 1 % i;
            case "english":
                return 2 % i;
            case "algorithm":
                return 3 % i;
            default:
                return 4 % i;
        }
    }
}