package com.base.partitionEmployee;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class partEmployeeMapper extends Mapper<LongWritable, Text, IntWritable, Employee> {
    protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {

        String data = value1.toString();
        String[] words = data.split(",");

        Employee e = new Employee();
        e.setEmpno(Integer.parseInt(words[0]));
        e.setEname(words[1]);
        e.setJob(words[2]);

        try {
            e.setMgr(Integer.parseInt(words[3]));
        } catch (Exception ex) {
            e.setMgr(-1);
        }

//            if(words[3]==""){
//                e.setMgr(-1);
//            }else{
//                e.setMgr(Integer.parseInt(words[3]));
//            }
        e.setHiredate(words[4]);
        e.setSal(Integer.parseInt(words[5]));
        try {
            e.setComn(Integer.parseInt(words[6]));
        } catch (Exception ex) {
            e.setComn(0);
        }
        e.setDeptno((Integer.parseInt(words[7])));
        context.write(new IntWritable(e.getDeptno()), e);

    }

}
