package com.projects.emp;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class empPartition extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int numPartitions) {
//        String[] line = text.toString().split(",");
//        String s = line[5];

        //定义分区规则
//        String s = text.toString().split(",")[5];
//        if (Integer.parseInt(s) < 1500) {
//            return 1 % numPartitions;
//        } else if (Integer.parseInt(s) > 3000) {
//            return 2 % numPartitions;
//        } else {
//            return 3 % numPartitions;
//        }

        //以部门号分区
        String s = text.toString().split(",")[7];

        if (Integer.parseInt(s) == 10) {
            return 1 % numPartitions;
        } else if (Integer.parseInt(s) == 20) {
            return 2 % numPartitions;
        } else {
            return 3 % numPartitions;
        }
    }
}
