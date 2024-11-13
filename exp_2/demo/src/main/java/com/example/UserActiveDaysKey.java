package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserActiveDaysKey implements Comparable<UserActiveDaysKey> {
    private Text userId;
    private IntWritable activeDays;

    public UserActiveDaysKey(Text userId, IntWritable activeDays) {
        this.userId = userId;
        this.activeDays = activeDays;
    }

    public Text getUserId() {
        return userId;
    }

    public IntWritable getActiveDays() {
        return activeDays;
    }

    @Override
    public int compareTo(UserActiveDaysKey other) {
        // 按活跃天数降序排序
        return other.activeDays.compareTo(this.activeDays);
    }
}
