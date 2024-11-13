package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ActiveDaysMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private Text userId = new Text();
    private boolean isHeader = true;
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 跳过文件的第一行（列头）
        // 如果是第一行（表头），跳过处理
        if (isHeader) {
            isHeader = false;  // 标记表头已经处理过
            return;
        }

        // 按照逗号分隔字段
        String[] fields = value.toString().split(",");
        
        String userIdStr = fields[0].trim(); // 第一列是用户ID
        String directPurchaseStr = fields[5].trim(); // 第六列是 direct_purchase_amt
        String totalRedeemStr = fields[8].trim(); // 第九列是 total_redeem_amt

        // 判断该用户当天是否活跃
        double directPurchase = Double.parseDouble(directPurchaseStr);
        double totalRedeem = Double.parseDouble(totalRedeemStr);
        
        // 如果直接购买金额 > 0 或 赎回金额 > 0，则认为该用户当天活跃
        if (directPurchase > 0 || totalRedeem > 0) {
            userId.set(userIdStr);
            context.write(userId, one); // 输出用户ID和1，表示这一天活跃
        }
    }
}
