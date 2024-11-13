package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ActiveDaysReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private List<UserActiveDaysKey> userList = new ArrayList<>();  // 存储所有用户的 key-value 对
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int activeDays = 0;
        
        // 累计每个用户的活跃天数
        for (IntWritable value : values) {
            activeDays += value.get(); // 每次出现1，表示该用户当天活跃
        }
        
        // 将每个用户的活跃天数和对应的 key 存入列表
        UserActiveDaysKey userKey = new UserActiveDaysKey(new Text(key), new IntWritable(activeDays));
        userList.add(userKey);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 在 reduce 方法结束后对收集的数据进行排序，并输出
        sortAndOutput(context);
    }

    private void sortAndOutput(Context context) throws IOException, InterruptedException {
        // 按活跃天数降序排序
        Collections.sort(userList, (user1, user2) -> user2.getActiveDays().compareTo(user1.getActiveDays()));

        // 输出排序后的结果
        for (UserActiveDaysKey userActiveDaysKey : userList) {
            context.write(userActiveDaysKey.getUserId(), userActiveDaysKey.getActiveDays());
        }
    }
}
