package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FlowJob <input path> <output path>");
            System.exit(-1);
        }

        // 配置MapReduce作业
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Daily Flow");
        job.setJarByClass(FlowJob.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 设置Mapper的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowWritable.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

