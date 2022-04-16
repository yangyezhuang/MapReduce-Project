package com.base.partitionEmployee;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyEmployeeParitioner extends Partitioner<IntWritable, Employee> {
    public int getPartition(IntWritable k2, Employee v2, int numPartition) {

//        if (v2.getSal() < 1500) {
//            return 1 % numPartition;
//        } else if (v2.getSal() >= 1500 && v2.getSal() < 3000) {
//            return 2 % numPartition;
//        } else {
//            return 3 % numPartition;
//        }
//
//
//        if (v2.getDeptno() == 10) {
//            return 1 % numPartition;
//        } else if (v2.getDeptno() == 20) {
//            return 2 % numPartition;
//        } else {
//            return 3 % numPartition;
//        }

        if (k2.get() == 10) {
            return 1 % numPartition;
        } else if (k2.get() == 20) {
            return 2 % numPartition;
        } else {
            return 3 % numPartition;
        }

    }
}
