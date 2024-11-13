package com.example;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;


import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InterestFlowMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private Map<String, String> dateToFlowMap = new HashMap<>(); // 存储 output_1 文件的日期到流入流出的映射
    private double minInterest = 1.0993; // 万份收益的最小值
    private double maxInterest = 1.822; // 万份收益的最大值
    private boolean isHeader = true;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 使用Hadoop的FileSystem来读取output_1文件
        String OutputFile = context.getConfiguration().get("input/output_1");
        
        // 确保路径不为 null
        Path path = new Path(OutputFile);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        
        // 读取 output_1 文件
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        while ((line = br.readLine()) != null) {
            // 按照TAB字符分割日期和资金流入流出信息
            String[] parts = line.split("\t");
             // 提取日期
             String date = parts[0].trim();
             String[]parts_2=parts[1].split(",");
             // 检查流入流出信息是否符合预期格式（即是否有两个部分）
             String flowIn = parts_2[0].trim();  // 资金流入量
             String flowOut = parts_2[1].trim(); // 资金流出量
            
             // 将日期与Flow对象存入映射
             dateToFlowMap.put(date, flowIn+","+flowOut);
        }


    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(","); // csv 格式
        
        if (isHeader) {
            isHeader = false;  // 标记表头已经处理过
            return;
        }
        if (fields.length >= 2) {
            String date = fields[0].trim(); // 日期
            double interest = Double.parseDouble(fields[1].trim()); // 万份收益
            
            // 获取该日期的资金流入和流出信息
            String flowInfo = dateToFlowMap.get(date);
            if (flowInfo != null) {
                // 根据万份收益计算区间
                int level = calculateLevel(interest);
                // 输出键值对：区间等级 -> 流入流出数据
                context.write(new IntWritable(level), new Text(flowInfo));
            }
        }
    }

    // 计算万份收益值所属的区间
    private int calculateLevel(double interest) {
        // 计算区间宽度
        double intervalWidth = (maxInterest - minInterest) / 10;
        
        // 计算兴趣值属于哪个区间
        int level = (int) ((interest - minInterest) / intervalWidth);
        if (level == 10) {
            // 防止超过最大区间
            level = 9;
        }
        return level;
    }
    
}
