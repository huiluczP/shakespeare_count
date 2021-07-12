package com.huiluczP.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// 逆转输出
public class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    protected void reduce(IntWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
        for(Text value : values){
            context.write(value, key);
        }
    }
}
