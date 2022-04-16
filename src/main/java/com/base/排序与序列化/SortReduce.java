package com.basics.排序与序列化;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {
    //  reduce方法将新的k2，v2转化为k3，v3
    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
