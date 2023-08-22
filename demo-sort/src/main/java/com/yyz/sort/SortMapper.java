package com.yyz.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
    //map方法将k1，v1转为k2，v2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //  1.将文本数据（v1）拆分，并将数据封装到SortBean对象，得到k2
        String[] split = value.toString().split(",");

        SortBean sortBean = new SortBean();
        sortBean.setWord(split[0]);
        sortBean.setNum(Integer.parseInt(split[1]));

        //  2.将k2和v2写入到上下文中
        context.write(sortBean, NullWritable.get());
    }
}
