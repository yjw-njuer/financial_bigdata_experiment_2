package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.example.InterestFlowMapper;
public class InterestFlowDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("input/output_1",args[2]);
        Job job = Job.getInstance(conf, "Interest Flow Analysis");

        job.setJarByClass(InterestFlowDriver.class);
        job.setMapperClass(InterestFlowMapper.class);
        job.setReducerClass(InterestFlowReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

