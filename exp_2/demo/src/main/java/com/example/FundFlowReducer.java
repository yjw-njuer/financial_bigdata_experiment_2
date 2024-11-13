package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

public class FundFlowReducer extends Reducer<Text, Text, Text, Text> {

    // 初始化日期格式化对象
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    // 用于存储每一天的流入量和流出量的集合
    private List<DayFlow> dayFlows = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 用来累计每一天（星期一到星期日）的资金流入和流出
        long[] inFlows = new long[7];  // 累计资金流入：星期一至星期日
        long[] outFlows = new long[7]; // 累计资金流出：星期一至星期日
        int[] counts = new int[7];     // 每天的计数，用于计算平均值

        // 遍历所有值（每个值包含日期，流入和流出）
        for (Text value : values) {
            String[] flows = value.toString().split(",");
            // 解析数据
            long inFlow = Long.parseLong(flows[0].trim());  // 流入
            long outFlow = Long.parseLong(flows[1].trim()); // 流出
            int dayOfWeek = getWeekNum(key.toString());
            // 累计该星期几的流入、流出和计数
            inFlows[dayOfWeek] += inFlow;
            outFlows[dayOfWeek] += outFlow;
            counts[dayOfWeek] += 1;
        }

        // 计算并将每一天（星期一至星期日）的平均流入和流出加入到 dayFlows 列表中
        for (int i = 0; i < 7; i++) {
            if (counts[i] > 0) {
                long avgInFlow = inFlows[i] / counts[i];
                long avgOutFlow = outFlows[i] / counts[i];
                dayFlows.add(new DayFlow(i, avgInFlow, avgOutFlow));  // 将 DayFlow 对象加入列表
            }
        }
    }

    // cleanup 方法用于进行排序并输出
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 按照流入量从大到小排序
        Collections.sort(dayFlows, Collections.reverseOrder());

        // 输出排序后的结果
        for (DayFlow dayFlow : dayFlows) {
            String dayOfWeekName = getDayOfWeekName(dayFlow.getDayOfWeek());
            context.write(new Text(dayOfWeekName), new Text(dayFlow.getInFlow() + "," + dayFlow.getOutFlow()));
        }
    }

    // 将星期几的索引（0-6）转为星期几的名称
    private String getDayOfWeekName(int dayIndex) {
        switch (dayIndex) {
            case 0: return "Sun.";
            case 1: return "Mon.";
            case 2: return "Tues.";
            case 3: return "Wed.";
            case 4: return "Thur.";
            case 5: return "Fri.";
            case 6: return "Sat.";
            default: return "Unknown";
        }
    }

    private int getWeekNum(String Weekday) {
        switch (Weekday) {
            case "Sunday": return 0;
            case "Monday": return 1;
            case "Tuesday": return 2;
            case "Wednesday": return 3;
            case "Thursday": return 4;
            case "Friday": return 5;
            case "Saturday": return 6;
            default: return 7;
        }
    }

    // 用于存储每一天的流入量和流出量
    public static class DayFlow implements Comparable<DayFlow> {
        private int dayOfWeek;
        private long inFlow;
        private long outFlow;

        public DayFlow(int dayOfWeek, long inFlow, long outFlow) {
            this.dayOfWeek = dayOfWeek;
            this.inFlow = inFlow;
            this.outFlow = outFlow;
        }

        public int getDayOfWeek() {
            return dayOfWeek;
        }

        public long getInFlow() {
            return inFlow;
        }

        public long getOutFlow() {
            return outFlow;
        }

        // 实现 Comparable 接口，按流入量从大到小排序
        @Override
        public int compareTo(DayFlow other) {
            return Long.compare(other.inFlow, this.inFlow);  // 降序排序
        }
    }
}
