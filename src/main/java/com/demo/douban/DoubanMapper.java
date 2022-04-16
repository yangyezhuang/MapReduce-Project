package com.projects.douban;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;

public class DoubanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    String name = null;
    String author = null;
    String publisher = null;
    String date = null;
    String price = null;
    String rate = null;
    String comm_num = null;
    String type = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        line = line.split("}")[0] + "}";
//        System.out.println(line);

        try {
            JSONObject jsonObject = JSON.parseObject(line);

            if (jsonObject.containsKey("name")) {
                name = jsonObject.getString("name");
            }
            if (jsonObject.containsKey("author")) {
                author = jsonObject.getString("author");
            }
            if (jsonObject.containsKey("publisher")) {
                publisher = jsonObject.getString("publisher");
            }
            if (jsonObject.containsKey("rate")) {
                rate = jsonObject.getString("rate");
            }
            if (jsonObject.containsKey("comm_num")) {
                comm_num = jsonObject.getString("comm_num").replace("人评价", "");
            }
            if (jsonObject.containsKey("type")) {
                type = jsonObject.getString("type");
            }

            /**
             * 清洗日期
             */
            if (jsonObject.containsKey("date")) {
//                boolean status = jsonObject.getString("date").contains("年");
                String str_date = jsonObject.getString("date");
                if (str_date.contains("年")) {
                    date = str_date.replaceAll("(?:年|月|日)", "-").substring(0, str_date.length() - 1);
                }
//                if (status) {
//                    String data = jsonObject.getString("date");
//                    date = data.replaceAll("(?:年|月|日)", "-").substring(0, data.length() - 1);
//                }
                else if (str_date.contains("元")) {
                    context.getCounter(DoubanCount.TotalRecorder).increment(1);
                    return;
                } else {
                    int date_len = str_date.length();

                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
                    SimpleDateFormat df4 = new SimpleDateFormat("yyyy");
                    SimpleDateFormat df6_1 = new SimpleDateFormat("yyyy/M", Locale.ENGLISH);
                    SimpleDateFormat df6_2 = new SimpleDateFormat("yyyy.M", Locale.ENGLISH);
                    SimpleDateFormat df8_1 = new SimpleDateFormat("yyyy-MMM", Locale.ENGLISH);
                    SimpleDateFormat df8_2 = new SimpleDateFormat("MMM,yyyy", Locale.ENGLISH);
                    SimpleDateFormat df8_3 = new SimpleDateFormat("yyyy/M/d", Locale.ENGLISH);
                    SimpleDateFormat df11_1 = new SimpleDateFormat("MMM dd,yyyy", Locale.ENGLISH);
                    SimpleDateFormat df11_2 = new SimpleDateFormat("MMM-yyyy-dd", Locale.ENGLISH);
                    SimpleDateFormat df11_3 = new SimpleDateFormat("dd MMM,yyyy", Locale.ENGLISH);
                    SimpleDateFormat df13 = new SimpleDateFormat("MMM-yyyy-dd", Locale.ENGLISH);
                    try {
                        switch (date_len) {
                            case 4: {
                                Date parse = df4.parse(str_date);
                                String format = df.format(parse);
                                date = format;
                                break;
                            }
                            case 6: {
                                if (str_date.contains("/")) {
                                    Date parse = df6_1.parse(str_date);
                                    String format = df.format(parse);
                                    date = format;
                                } else {
                                    Date parse = df6_2.parse(str_date);
                                    String format = df.format(parse);
                                    date = format;
                                }
                                break;
                            }
                            case 8: {
                                if (str_date.contains("-")) {
                                    Date parse = df8_1.parse(str_date);
                                    String format = df.format(parse);
                                    date = format;
                                } else if (str_date.contains(",")) {
                                    Date parse = df8_2.parse(str_date);
                                    String format = df.format(parse);
                                    date = format;
                                } else {
                                    Date parse = df8_3.parse(str_date);
                                    String format = df.format(parse);
                                    date = format;
                                }
                                break;
                            }
                            case 11: {
                                if (str_date.contains("-")) {
                                    Date parse = df11_2.parse(str_date);
                                    String format = df.format(parse);
                                    date = format;
                                } else {
                                    if (str_date.substring(2, 3).equals(" ")) {
                                        Date parse = df11_3.parse(str_date);
                                        String format = df.format(parse);
                                        date = format;
                                    } else {
                                        Date parse = df11_1.parse(str_date);
                                        String format = df.format(parse);
                                        date = format;
                                    }
                                }
                                break;
                            }
                            case 13: {
                                if (str_date.contains("rd")) {
                                    String d = str_date.replace("rd", "");
                                    Date parse = df11_1.parse(d);
                                    String format = df.format(parse);
                                    date = format;
                                } else {
                                    Date parse = df13.parse(str_date);
                                    String format = df.format(parse);
                                    date = format;
                                }
                                break;
                            }
                        }
                    } catch (Exception e) {
                        date = jsonObject.getString("date");
                    }
                }
            }


            /**
             * 清洗货币
             */
            if (jsonObject.containsKey("price")) {
                //查看货币种类
//                price = jsonObject.getString("price").replaceAll("[0-9.元]", "");
//                if (price == null){
//                    return;}
//                else System.out.println(price);

                String str_price = jsonObject.getString("price");

                if (str_price.contains("NT")) {
                    String re = "[^0-9||.]";
                    Pattern p = Pattern.compile(re);
                    Matcher m = p.matcher(str_price);
                    String prc = m.replaceAll("");
                    int pri = Integer.parseInt(prc) / 4;
                    price = String.valueOf(pri);
                } else if (str_price.contains("CNY")) {
                    String re = "[^0-9||.]";
                    Pattern p = Pattern.compile(re);
                    Matcher m = p.matcher(str_price);
                    String prc = m.replaceAll("");
                    float pri = Float.parseFloat(prc);
                    price = String.valueOf(pri);
                } else if (str_price.contains("GBP")) {
                    String re = "[^0-9||.]";
                    Pattern p = Pattern.compile(re);
                    Matcher m = p.matcher(str_price);
                    String prc = m.replaceAll("");
                    float pri = Float.parseFloat(prc);// 英镑
                    price = String.valueOf(pri * 8.5);
                } else if (str_price.contains("JPY")) {
                    String re = "[^0-9||.]";
                    Pattern p = Pattern.compile(re);
                    Matcher m = p.matcher(str_price);
                    String prc = m.replaceAll("");
                    int pri = Integer.parseInt(prc);
                    price = String.valueOf(pri / 16);
                } else if (str_price.contains("USD")) {
                    String re1 = "[^0-9||.]";
                    Pattern p1 = Pattern.compile(re1);
                    Matcher m = p1.matcher(str_price);
                    String prc1 = m.replaceAll("");
                    float pri = Float.parseFloat(prc1);
                    price = String.valueOf(pri * 7);
                } else if (str_price.contains("元")) {
                    String re = "[^0-9||.]";
                    Pattern p = Pattern.compile(re);
                    Matcher m = p.matcher(str_price);
                    String prc = m.replaceAll("");
                    float pri = Float.parseFloat(prc);
                    price = String.valueOf(pri);
                } else if (str_price.contains("-")) {
                    context.getCounter(DoubanCount.TotalRecorder).increment(1);
                    return;
                } else if (str_price.contains("TWD")) {
                    String re1 = "[^0-9||.]";
                    Pattern p1 = Pattern.compile(re1);
                    Matcher m = p1.matcher(str_price);
                    String prc = m.replaceAll("");
                    int pri = Integer.parseInt(prc);
                    price = String.valueOf(pri / 4);
                } else if (str_price.contains("大塊文化")) {
                    context.getCounter(DoubanCount.TotalRecorder).increment(1);
                    return;
                } else if (str_price.contains("（全三册）")) {
                    price = str_price.replaceAll("（全三册）", "");
                } else if (str_price.equals("")) {
                    context.getCounter(DoubanCount.TotalRecorder).increment(1);
                    return;
                } else {
//                    price = jsonObject.getString("price").replace("元", "");
                    price = jsonObject.getString("price");
                }
            }
            String result = toString();
            context.write(new Text(result), NullWritable.get());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String toString() {
        return name + "|" +
                author + "|" +
                publisher + "|" +
                date + "|" +
                price + "|" +
                rate + "|" +
                comm_num + "|" +
                type;
    }
}
