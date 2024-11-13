package com.example;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<FlowWritable> values, Context context) throws IOException, InterruptedException {
        double totalPurchase = 0;
        double totalRedeem = 0;

        // 累加所有流入和流出金额
        for (FlowWritable flow : values) {
            totalPurchase += flow.getPurchaseAmount().get();
            totalRedeem += flow.getRedeemAmount().get();
        }

        // 输出日期和对应的资金流入、流出情况
        context.write(key, new Text(totalPurchase + "," + totalRedeem));
    }
}
