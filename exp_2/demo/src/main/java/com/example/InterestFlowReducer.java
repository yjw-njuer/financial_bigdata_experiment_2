package com.example;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import com.example.InterestFlowMapper;
import java.io.IOException;

public class InterestFlowReducer extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double[] totalFlowIn = new double[10];
        double[] totalFlowOut = new double[10];
        int[] count = new int[10];

        for (Text val : values) {
            String flowData = val.toString();
            String[] flowInfo = flowData.split(",");
            double flowIn = Double.parseDouble(flowInfo[0]);
            double flowOut = Double.parseDouble(flowInfo[1]);
            int level = getLevelNum(key.toString());
            totalFlowIn[level] += flowIn;
            totalFlowOut[level] += flowOut;
            count[level]++;
        }
        for(int i=0;i<=9;i++){
            if (count[i] > 0) {
                // 计算平均资金流入和流出
                double avgFlowIn = totalFlowIn[i] / count[i];
                double avgFlowOut = totalFlowOut[i] / count[i];
                String outputValue = String.format("%.2f\t%.2f", avgFlowIn, avgFlowOut);
                context.write(new Text("Level " + key.get() + " (" + getRangeForLevel(key.get()) + ")"), new Text(outputValue));
                
            }
        }
    }
    private String getRangeForLevel(int level) {
        // 计算万份收益等级的区间
        double minInterest = 1.0993;
        double maxInterest = 1.822; 
        double interval = (maxInterest - minInterest) / 10;

        double lowerBound = minInterest + level * interval;
        double upperBound = minInterest + (level + 1) * interval;

        return String.format("%.2f - %.2f", lowerBound, upperBound);
    }
    private int getLevelNum(String level) {
        switch (level) {
            case "0": return 0;
            case "1": return 1;
            case "2": return 2;
            case "3": return 3;
            case "4": return 4;
            case "5": return 5;
            case "6": return 6;
            case "7": return 7;
            case "8": return 8;
            case "9": return 9;
            default: return 10;
        }
    }

}

