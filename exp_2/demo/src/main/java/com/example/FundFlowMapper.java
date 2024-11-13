package com.example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.Date;

public class FundFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 输入格式: 日期 TAB 资金流入量,资金流出量
        String line = value.toString().trim();
        String[] parts = line.split("\t");
        

        String date = parts[0];  // 日期
        String[] flows = parts[1].split(",");

        // 解析日期并计算星期几
        int dayOfWeek = getDayOfWeek(date);
        // 解析资金流入和流出量
        String weekname=getDayOfWeekName(dayOfWeek);
        double inFlow_double = Double.parseDouble(flows[0].trim());  // 转换为双精度浮点数
        long inFlow=(long)inFlow_double;
        double outFlow_double = Double.parseDouble(flows[1].trim());
        long outFlow=(long)outFlow_double;


        // 发送到 Reducer
        context.write(new Text(weekname), new Text(inFlow + "," + outFlow));
    }
    // 将日期字符串转化为星期几的索引（0-6）
    private int getDayOfWeek(String dateString) {
         try {
            
            Date date = dateFormat.parse(dateString);  // 解析日期字符串
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            return calendar.get(Calendar.DAY_OF_WEEK) - 1;  // Calendar的星期天为1, 星期一为2, 所以减去1
        } catch (ParseException e) {
            // 处理解析异常，输出错误日志，返回-1作为默认值
            e.printStackTrace();
            return -1;  // 如果日期解析失败，返回-1
        }
    }

    // 将星期几的索引（0-6）转为星期几的名称
    private String getDayOfWeekName(int dayIndex) {
        switch (dayIndex) {
            case 0: return "Sunday";
            case 1: return "Monday";
            case 2: return "Tuesday";
            case 3: return "Wednesday";
            case 4: return "Thursday";
            case 5: return "Friday";
            case 6: return "Saturday";
            default: return "Unknown";
        }
    }
}
