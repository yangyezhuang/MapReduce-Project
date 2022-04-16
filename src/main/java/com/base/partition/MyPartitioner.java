package com.base.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区案例
 */
public class MyPartitioner extends Partitioner<Text, NullWritable> {
    /*
      1：定义分区规则
      2：返回对应的分区编号
    */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        // 1：拆分文本数据（k2）
//        String[] line = text.toString().split("|");
//        String s = line[2];
        String s = text.toString().split("=")[4];

        // 2：判断
        if (Float.parseFloat(s) > 30) {
            return 1;
//            context.getCounter(DoubanCount.TotalRecorder).increment(1);
        } else {
            return 0;
        }
    }
}
