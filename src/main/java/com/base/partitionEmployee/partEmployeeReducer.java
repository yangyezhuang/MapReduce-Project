package com.base.partitionEmployee;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class partEmployeeReducer extends Reducer<IntWritable, Employee, IntWritable, Employee> {
    protected void reduce(IntWritable k3, Iterable<Employee> v3, Context context) throws IOException, InterruptedException {

        for (Employee e : v3) {
            context.write(k3, e);
        }
    }
}
