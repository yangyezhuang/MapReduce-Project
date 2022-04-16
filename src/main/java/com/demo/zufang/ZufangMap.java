package com.projects.zufang;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ZufangMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    String city = null;
    String address = null;
    String apartment = null;
    String areas = null;
    String supportingFacility = null;
    String rent = null;
    String time = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//        JobMain jobMain = new JobMain();

        boolean hasRent = true;
        String contain = value.toString();
//        if (contain.contains("},")){
//            contain = contain.split("},")[0] + "}";
//        }
        contain = contain.split("},")[0] + "}";
        System.out.println(contain);
        try {
            JSONObject jsonObject = JSON.parseObject(contain);

            if (jsonObject.containsKey("city")) {
                city = jsonObject.getString("city");
            }
            if (jsonObject.containsKey("地址")) {
                address = jsonObject.getString("地址");
            }
            if (jsonObject.containsKey("户型")) {
                apartment = jsonObject.getString("户型");
            }
            if (jsonObject.containsKey("面积")) {
                areas = jsonObject.getString("面积");
            }
            if (jsonObject.containsKey("独用设施")) {
                supportingFacility = jsonObject.getString("独用设施");
            }
            if (jsonObject.containsKey("房租")) {
                rent = jsonObject.getString("房租");
                if (rent.equals("")) {
                    hasRent = false;
                    context.getCounter(count.TotalRecorder).increment(1);
                    //                JobMain.COUNT++;
                }
            }
            if (jsonObject.containsKey("更新")) {
                time = jsonObject.getString("更新");
            }
            String result = toString();
            if (hasRent) {
                context.write(new Text(result), NullWritable.get());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
//        return '{' + "city='" + city + '\'' +
//                ", address='" + address + '\'' +
//                ", apartment='" + apartment + '\'' +
//                ", areas='" + areas + '\'' +
//                ", supportingFacility='" + supportingFacility + '\'' +
//                ", rent='" + rent + '\'' +
//                ", time='" + time + '\'' +
//                '}';

        return city + '，' +
                address + '，' +
                apartment + '\'' +
                areas + '，' +
                supportingFacility + '，' +
                rent + '，' +
                time;
    }
}
