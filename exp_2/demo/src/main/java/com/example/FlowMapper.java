package com.example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowWritable> {
    // 用于标记是否是表头
    private boolean isHeader = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // 如果是第一行（表头），跳过处理
        if (isHeader) {
            isHeader = false;  // 标记表头已经处理过
            return;
        }

        // 解析 CSV 行
        String[] fields = line.split(",");
        
        // 获取日期、申购金额和赎回金额
        String date = fields[1].trim();
        double purchaseAmount = fields[4].isEmpty() ? 0.0 : Double.parseDouble(fields[4].trim());
        double redeemAmount = fields[8].isEmpty() ? 0.0 : Double.parseDouble(fields[8].trim());
        
        // 创建一个 FlowWritable 对象，将申购金额和赎回金额存入
        FlowWritable flow = new FlowWritable(purchaseAmount, redeemAmount);
        
        // 将日期作为键，FlowWritable 对象作为值
        context.write(new Text(date), flow);
    }
}
